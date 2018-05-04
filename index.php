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
    /*
    //SQlite
    $sql = \Sigma\BuildQuery::init('sqlite',__DIR__.'/sqlite/teste.db',null,null,null);
    */

    /*
    // Postgres
    $sql = \Sigma\BuildQuery::init('postgres','db','infosistemas','infosistemas','info1234');
    */

    /*
    // Firebird
    $sql = \Sigma\BuildQuery::init('firebird','192.168.0.205','/usr/infosistemas/dados.gdb','infosistemas','firebird');
    */
    // Mysql
    $sql = \Sigma\BuildQuery::init('mysql','192.168.1.32','mysql','root','');

    //$dados1 = $sql->tabela('Teste')->campos(['Id','Nome'], [6,"Alguem 2"])->buildQuery('insert');
    $dados = $sql
        ->tabela('user')
        ->campos(['*'])
        //->limit(3,5)
        ->buildQuery('select');
    print_r($dados);
} catch(Exception $e) {
    print_r($e->getMessage());
    echo PHP_EOL.$e->getCode();
}