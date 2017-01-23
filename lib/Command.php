<?php

namespace Amp\Stomp;

interface Command
{
    const ABORT         = "ABORT";
    const ACK           = "ACK";
    const BEGIN         = "BEGIN";
    const COMMIT        = "COMMIT";
    const CONNECT       = "CONNECT";
    const CONNECTED     = "CONNECTED";
    const DISCONNECT    = "DISCONNECT";
    const ERROR         = "ERROR";
    const MESSAGE       = "MESSAGE";
    const NACK          = "NACK";
    const RECEIPT       = "RECEIPT";
    const SEND          = "SEND";
    const STOMP         = "STOMP";
    const SUBSCRIBE     = "SUBSCRIBE";
    const UNSUBSCRIBE   = "UNSUBSCRIBE";
}
