<?php

/**
 * Created by PhpStorm.
 * User: alessandrominoccheri
 * Date: 2018-12-05
 * Time: 17:10
 */

namespace Broadway\EventStore\DynamoDb\Objects;


use Aws\ResultInterface;
use Broadway\Domain\DateTime;
use Broadway\Domain\DomainMessage;
use Broadway\Domain\Metadata;
use Broadway\Serializer\Serializer;

class DeserializeEvent
{
    /**
     * @param array<array<string,mixed>> $row
     */
    public static function deserialize(
        $row,
        Serializer $metadataSerializer,
        Serializer $payloadSerializer
    ): DomainMessage {
		/** @var array{uuid?:string,playhead?:string,payload?:string,metadata?:string,recorded_on?:string}|null */
		$eventData = ConvertAwsItemToArray::convert($row);

        if (null === $eventData) {
            throw new \Exception('EventData cannot be null');
        }

        $uuid = $eventData['uuid'] ?? throw new \Exception('uuid key not found');
        $playhead = $eventData['playhead'] ?? throw new \Exception('playhead key not found');
        $payload = $eventData['payload'] ?? throw new \Exception('payload key not found');
        $metadata = $eventData['metadata'] ?? throw new \Exception('metadata key not found');
        $recordedOn = $eventData['recorded_on'] ?? throw new \Exception('recorded_on key not found');

		/** @var array<string,mixed> $decodedMetadata */
		$decodedMetadata = json_decode($metadata, true);
		/** @var array<string,mixed> $decodedPayload */
		$decodedPayload = json_decode($payload, true);

		/** @var Metadata $deserializedMetadata */
		$deserializedMetadata = $metadataSerializer->deserialize($decodedMetadata);

        return new DomainMessage(
            $uuid,
            (int) $playhead,
            $deserializedMetadata,
            $payloadSerializer->deserialize($decodedPayload),
            DateTime::fromString($recordedOn)
        );
    }
}
