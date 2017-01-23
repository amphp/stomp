<?php

namespace Amp\Stomp;

interface Version
{
    const V_1_0 = 0b001;
    const V_1_1 = 0b010;
    const V_1_2 = 0b100;
    const V_ALL = 0b111;

    const STRINGS = [
        self::V_1_0 => "1.0",
        self::V_1_1 => "1.1",
        self::V_1_2 => "1.2",
    ];
}
