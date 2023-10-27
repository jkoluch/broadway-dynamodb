<?php

/**
 * Created by PhpStorm.
 * User: alessandrominoccheri
 * Date: 2018-12-06
 * Time: 11:00
 */

namespace Tests\Expressions;


use Broadway\EventStore\DynamoDb\Expressions\FilterExpression;
use PHPUnit\Framework\TestCase;

class FilterExpressionTest extends TestCase
{
    private FilterExpression $filterExpression;

    protected function setUp(): void
    {
        parent::setUp();
        $this->filterExpression = new FilterExpression();
    }

    public function testAddField(): void
    {
        $field = 'foo';

        $this->filterExpression->addField($field);

        $expected = '#' . $field . ' = :' . $field;

        self::assertEquals($expected, $this->filterExpression->getExpression());
    }

    public function testInFieldWithPosition(): void
    {
        $field = 'foo';
        $position = random_int(1, 9999);
        $positionExpected = $position + 1;

        $this->filterExpression->addInFieldWithPosition($field, $position);

        $expected = ':' . $field . $positionExpected;

        self::assertEquals($expected, $this->filterExpression->getExpression());
    }

    public function testAddConditionOperator(): void
    {
        $condition = 'or';

        $this->filterExpression->addConditionOperator($condition);

        $expected = ' or ';

        self::assertEquals($expected, $this->filterExpression->getExpression());
    }

    public function testAddInCondition(): void
    {
        $field = 'field';

        $this->filterExpression->addInCondition($field);

        $expected = '#' . $field . ' IN(';

        self::assertEquals($expected, $this->filterExpression->getExpression());
    }

    public function testAddComma(): void
    {
        $this->filterExpression->addComma();

        $expected = ', ';

        self::assertEquals($expected, $this->filterExpression->getExpression());
    }
}

