<?php

namespace Amp\Stomp;

final class NullFilter implements Filter
{
    public function filter(int $mode, Frame $frame): Frame
    {
        return $frame;
    }
}
