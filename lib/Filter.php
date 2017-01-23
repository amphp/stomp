<?php

namespace Amp\Stomp;

interface Filter
{
    const READ  = 0b01;
    const WRITE = 0b10;

    public function filter(int $mode, Frame $frame): Frame;
}
