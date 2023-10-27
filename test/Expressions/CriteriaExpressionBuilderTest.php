<?php
/**
 * Created by PhpStorm.
 * User: alessandrominoccheri
 * Date: 2018-12-05
 * Time: 15:47
 */

namespace Tests\Expressions;

use Broadway\EventStore\DynamoDb\Expressions\CriteriaExpressionBuilder;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

class CriteriaExpressionBuilderTest extends TestCase
{
    public function testCreateScanFilterWithSimpleCondition(): void
    {
        $randomInt = random_int(1, 999999);

        $fields = [
            'foo' => 'baz',
            'bar' => $randomInt
        ];

        $scanFilter = new CriteriaExpressionBuilder($fields);

        self::assertEquals('#foo = :foo and #bar = :bar', $scanFilter->getFilterExpression());
        self::assertEquals('{":foo":"baz",":bar":' . $randomInt . '}', $scanFilter->getExpressionAttributeValues());
        self::assertEquals(['#foo' => 'foo', '#bar' => 'bar'], $scanFilter->getExpressionAttributeNames());
    }

    public function testCreateScanFilterWithInCondition(): void
    {
        $id1 = Uuid::uuid4()->toString();
        $id2 = Uuid::uuid4()->toString();
        $fields = [];
        $fields['uuid'] = ['in' => [$id1, $id2]];

        $scanFilter = new CriteriaExpressionBuilder($fields);

        self::assertEquals('#uuid IN(:uuid1, :uuid2)', $scanFilter->getFilterExpression());
        self::assertEquals('{":uuid1":"'. $id1 . '",":uuid2":"' . $id2 . '"}', $scanFilter->getExpressionAttributeValues());
        self::assertEquals(['#uuid' => 'uuid'], $scanFilter->getExpressionAttributeNames());
    }
}
