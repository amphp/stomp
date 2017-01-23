<?php

namespace Amp\Stomp;

final class MultiFilter implements Filter
{
    private $filters;

    public function __construct(array $filters)
    {
        $this->filters = \array_map([$this, "validate"], $filters);
    }

    private function validate(Filter $f)
    {
        return $f;
    }

    public function filter(int $mode, Frame $frame): Frame
    {
        foreach ($this->filters as $filter) {
            $frame = $filter->filter($mode, $frame);
        }

        return $frame;
    }
}
