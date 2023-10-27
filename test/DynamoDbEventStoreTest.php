<?php

namespace Tests;

use Aws\DynamoDb\DynamoDbClient;
use Broadway\Domain\DomainEventStream;
use Broadway\Domain\DomainMessage;
use Broadway\EventStore\DynamoDb\DynamoDbEventStore;
use Broadway\EventStore\DynamoDb\Objects\DeserializeEvent;
use Broadway\EventStore\EventStreamNotFoundException;
use Broadway\EventStore\EventVisitor;
use Broadway\EventStore\Management\Criteria;
use Broadway\EventStore\Management\CriteriaNotSupportedException;
use Broadway\Serializer\ReflectionSerializer;

class DynamoDbEventStoreTest extends \PHPUnit\Framework\TestCase
{
    private DynamoDbEventStore $dynamoDbEventStore;

    protected function setUp(): void
    {
        parent::setUp();

        $dynamodb = new DynamoDbClient([
            'region'   => 'local',
            'debug' => true,
            'version'  => 'latest',
            'endpoint' => 'http://dynamodb:8000',
            'credentials' => [
                'key' => 'key',
                'secret' => 'secret',
            ],
        ]);

		/** @var array{TableNames:string[]}|null $tables */
        $tables = $dynamodb->listTables([]);

        if (isset($tables['TableNames'])) {
			foreach ($tables['TableNames'] as $dynamoDbTable) {
                $dynamodb->deleteTable([
                    'TableName' => $dynamoDbTable,
                ]);
            }
        }

        $dynamodb->createTable([
            'TableName' => 'dynamo_table',
            'AttributeDefinitions' => [
                [
                    'AttributeName' => 'id',
                    'AttributeType' => 'S'
                ],
                [
                    'AttributeName' => 'playhead',
                    'AttributeType' => 'N'
                ]
            ],
            'KeySchema' => [
                [
                    'AttributeName' => 'id',
                    'KeyType' => 'HASH'
                ],
                [
                    'AttributeName' => 'playhead',
                    'KeyType' => 'RANGE'
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits'    => 5,
                'WriteCapacityUnits' => 6
            ]
        ]);

        $this->dynamoDbEventStore = new DynamoDbEventStore(
            $dynamodb,
            new ReflectionSerializer(),
            new ReflectionSerializer(),
            'dynamo_table'
        );
    }

    public function testInsertMessageAndLoadIt(): void
    {
        $id =  \Ramsey\Uuid\Uuid::uuid4()->toString();
        $playhead = 0;
        $metadata = new \Broadway\Domain\Metadata(['id' => $id, 'foo' => 'bar']);
        $payload = new class ()
        {
        };
        $recordedOn = \Broadway\Domain\DateTime::now();

        $domainMessage = new DomainMessage(
            $id,
            $playhead,
            $metadata,
            $payload,
            $recordedOn
        );

        $eventStream = new DomainEventStream([$domainMessage]);

        $this->dynamoDbEventStore->append($id, $eventStream);

        $events = $this->dynamoDbEventStore->load($id);

        self::assertCount(1, $events);

        /** @var DomainMessage $event */
		foreach ($events as $event) {
            self::assertEquals($id, $event->getId());
            self::assertEquals($playhead, $event->getPlayhead());
            self::assertEquals($metadata, $event->getMetadata());
            self::assertEquals($payload, $event->getPayload());
            self::assertEquals($recordedOn, $event->getRecordedOn());
        }
    }

    public function testInsertMessageAndLoadFromPlayhead(): void
    {
        $id =  \Ramsey\Uuid\Uuid::uuid4()->toString();
        $playhead = random_int(1, 9999);
        $metadata = new \Broadway\Domain\Metadata(['id' => $id, 'foo' => 'bar']);
        $payload = new class ()
        {
        };
        $recordedOn = \Broadway\Domain\DateTime::now();

        $domainMessage = new DomainMessage(
            $id,
            $playhead,
            $metadata,
            $payload,
            $recordedOn
        );

        $eventStream = new DomainEventStream([$domainMessage]);

        $this->dynamoDbEventStore->append($id, $eventStream);

        $events = $this->dynamoDbEventStore->loadFromPlayhead($id, $playhead);

        self::assertCount(1, $events);

        /** @var DomainMessage $event */
		foreach ($events as $event) {
            self::assertEquals($id, $event->getId());
            self::assertEquals($playhead, $event->getPlayhead());
            self::assertEquals($metadata, $event->getMetadata());
            self::assertEquals($payload, $event->getPayload());
            self::assertEquals($recordedOn, $event->getRecordedOn());
        }
    }

    private function appendEvent(mixed $id): DomainEventStream
    {
        $playhead = random_int(1, 9999);
        $metadata = new \Broadway\Domain\Metadata(['id' => $id, 'foo' => 'bar']);
        $payload = new class ()
        {
        };
        $recordedOn = \Broadway\Domain\DateTime::now();

        $domainMessage = new DomainMessage(
            $id,
            $playhead,
            $metadata,
            $payload,
            $recordedOn
        );

        return new DomainEventStream([$domainMessage]);
    }

    public function testInsertMessageAndVisitEvents(): void
    {
        $id =  \Ramsey\Uuid\Uuid::uuid4()->toString();
        $id2 =  \Ramsey\Uuid\Uuid::uuid4()->toString();

        $eventStream = $this->appendEvent($id);
        $this->dynamoDbEventStore->append($id, $eventStream);

        $eventStream = $this->appendEvent($id2);
        $this->dynamoDbEventStore->append($id2, $eventStream);

        $criteria = Criteria::create()->withAggregateRootIds([
            $id,
            $id2,
        ]);

        $eventVisitor = new RecordingEventVisitor();

        $this->dynamoDbEventStore->visitEvents($criteria, $eventVisitor);
		$items = $this->dynamoDbEventStore->getItems();
		/** @var array<array<array<string,mixed>>> $events */
		$events = $items['Items'] ?? [];

        self::assertCount(2, $events);

        foreach ($events as $event) {
            $eventDeserialized = DeserializeEvent::deserialize($event, new ReflectionSerializer(), new ReflectionSerializer());
            self::assertTrue($eventDeserialized->getId() === $id || $eventDeserialized->getId() === $id2);
        }
    }

    public function testEmptyEventsThrowExceptionOnLoad(): void
    {
        $id =  \Ramsey\Uuid\Uuid::uuid4()->toString();

        $eventStream = new DomainEventStream([]);

        $this->dynamoDbEventStore->append($id, $eventStream);

        $this->expectException(EventStreamNotFoundException::class);

        $this->dynamoDbEventStore->load($id);
    }

    public function testEmptyEventsThrowExceptionOnLoadFromPlayhead(): void
    {
        $id =  \Ramsey\Uuid\Uuid::uuid4()->toString();
        $playhead = random_int(1, 9999);

        $eventStream = new DomainEventStream([]);

        $this->dynamoDbEventStore->append($id, $eventStream);

        $this->expectException(EventStreamNotFoundException::class);

        $this->dynamoDbEventStore->loadFromPlayhead($id, $playhead);
    }

    public function testInsertMessageAndVisitEventsWithAggregateRootTypesThrowException(): void
    {
        $id =  \Ramsey\Uuid\Uuid::uuid4()->toString();
        $id2 =  \Ramsey\Uuid\Uuid::uuid4()->toString();

        $eventStream = $this->appendEvent($id);
        $this->dynamoDbEventStore->append($id, $eventStream);

        $eventStream = $this->appendEvent($id2);
        $this->dynamoDbEventStore->append($id2, $eventStream);

        $criteria = Criteria::create()->withAggregateRootTypes([
            'type1',
            'type2',
        ]);

        $eventVisitor = new RecordingEventVisitor();

        $this->expectException(CriteriaNotSupportedException::class);

        $this->dynamoDbEventStore->visitEvents($criteria, $eventVisitor);
    }
}

class RecordingEventVisitor implements EventVisitor
{
    public function doWithEvent(DomainMessage $domainMessage): void
    {
    }
}

