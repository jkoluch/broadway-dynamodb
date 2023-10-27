<?php

namespace Tests\Objects;

use Broadway\EventStore\DynamoDb\Objects\ConvertAwsItemToArray;
use PHPUnit\Framework\TestCase;

/**
 * Created by PhpStorm.
 * User: alessandrominoccheri
 * Date: 2018-12-28
 * Time: 18:23
 */

class ConvertAwsItemToArrayTest extends TestCase
{
    public function testReturnNullIfItemIsEmpty(): void
    {
        $resultConverted = ConvertAwsItemToArray::convert(null);
        self::assertNull($resultConverted);
    }

    public function testThrowExceptionIfTypeNotFound(): void
    {
        $this->expectException(\Exception::class);
        ConvertAwsItemToArray::convert([['notExistingKey' => 'foo']]);
    }

    public function testPassCorrectKeyReturnData(): void
    {
        $itemValue = 'foo';
        $resultConverted = ConvertAwsItemToArray::convert([['S' => $itemValue]]);

        self::assertEquals([$itemValue], $resultConverted);
    }
}
