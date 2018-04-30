<?php
/**
 * Created by Nathan Feitoza.
 * User: dev01
 * Date: 30/04/18
 * Time: 08:51
 */
require 'vendor/autoload.php';

echo'<pre>';
try {
    $sql = \Sigma\BuildQuery::init('postgres','db','db_teste','infosistemas','info1234');
    $dados = $sql
        ->tabela('table_teste')
        ->campos(['*'])
        ->buildQuery('select');
    print_r($dados);
} catch(Exception $e) {
    print_r($e->getMessage());
    echo PHP_EOL.$e->getCode();
}