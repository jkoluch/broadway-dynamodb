<?php
/**
 * Created by PhpStorm.
 * User: alessandrominoccheri
 * Date: 2018-12-05
 * Time: 10:20
 */

namespace Broadway\EventStore\DynamoDb\Objects;

class ConvertAwsItemToArray
{
	/** @var string[] */
	private static array $keyMap = ['S', 'SS', 'N', 'NS', 'B', 'BS'];

    /**
     * @param array<array<string,mixed>>|null $item
	 *
	 * @return array<string,mixed>|null
     */
    public static function convert($item): ?array
    {
        if ($item === null) {
            return null;
        }

        $converted = [];
        foreach ($item as $k => $v) {
            $keyFound = false;
            foreach (self::$keyMap as $key) {
                if (isset($v[$key])) {
                    $converted[$k] = $v[$key];
                    $keyFound = true;
                }
            }

            if (!$keyFound) {
                throw new \Exception('Not implemented type');
            }
        }

        return $converted;
    }
}
