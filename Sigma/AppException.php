<?php
/**
 * Class extends Exception for build personalizated errors
 */

namespace Sigma;

use Exception;

class AppException extends Exception {

    public function __construct($message, $code = 0, Exception $previous = null) {
        // garante que tudo está corretamente inicializado
        parent::__construct($message, $code, $previous);
    }
}