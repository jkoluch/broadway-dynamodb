<?php

/**
 * Created by PhpStorm.
 * User: alessandrominoccheri
 * Date: 2018-12-06
 * Time: 10:50
 */

namespace Tests\Expressions;


use Broadway\EventStore\DynamoDb\Expressions\ExpressionAttributeValues;
use PHPUnit\Framework\TestCase;

class ExpressionAttributeValuesTest extends TestCase
{
    private ExpressionAttributeValues $expressionAttributeValues;

    protected function setUp(): void
    {
        parent::setUp();
        $this->expressionAttributeValues = new ExpressionAttributeValues();
    }

    public function testCreateExpressionAttributeValues(): void
    {

        self::assertEquals('{', $this->expressionAttributeValues->getExpression());
    }

    public function testAddFieldWithPosition(): void
    {
        $field = 'foo';
        $position = random_int(1, 999);
        $positionExpected = $position + 1;
        $value = 'bar';

        $this->expressionAttributeValues->addFieldWithPosition($field, $position, $value);

        self::assertEquals('{":foo' . $positionExpected . '":"bar"', $this->expressionAttributeValues->getExpression());
    }

    public function testAddField(): void
    {
        $field = 'foo';
        $value = 'bar';

        $this->expressionAttributeValues->addField($field, $value);

        self::assertEquals('{":foo":"bar"', $this->expressionAttributeValues->getExpression());
    }

    public function testAddComma(): void
    {
        $this->expressionAttributeValues->addComma();

        self::assertEquals('{,', $this->expressionAttributeValues->getExpression());
    }

    public function testCloseExpression(): void
    {
        $this->expressionAttributeValues->close();

        self::assertEquals('{}', $this->expressionAttributeValues->getExpression());
    }
}

