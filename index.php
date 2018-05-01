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
    $sql = \Sigma\BuildQuery::init('mysql','localhost','nfe','root','123456');
    $dados = $sql
        ->tabela('clientes')
        ->campos(['*'])
        ->limit(3,5)
        ->buildQuery('select');
    print_r($dados);
} catch(Exception $e) {
    print_r($e->getMessage());
    echo PHP_EOL.$e->getCode();
}