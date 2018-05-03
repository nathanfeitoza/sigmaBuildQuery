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
    $sql = \Sigma\BuildQuery::init('sqlite','C:\Users\1171139648\Downloads\sigmaBuildQuery\sqlite\teste.db',null,null,null);
	//$dados1 = $sql->tabela('Teste')->campos(['Id','Nome'], [6,"Alguem 2"])->buildQuery('insert');
    $dados = $sql
        ->tabela('Teste')
        ->campos(['*'])
        //->limit(3,5)
        ->buildQuery('select');
    print_r($dados);
} catch(Exception $e) {
    print_r($e->getMessage());
    echo PHP_EOL.$e->getCode();
}