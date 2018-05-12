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
    $sql = \Sigma\BuildQuery::init('postgres','db','banco','usuario','senha');
    */


    // Firebird
    $sql = \Sigma\BuildQuery::init('postgres','localhost','teste','postgres','123456');

    /*
    // Mysql
    $sql = \Sigma\BuildQuery::init('mysql','192.168.1.32','mysql','root','');
    */
    //$dados1 = $sql->tabela('Teste')->campos(['Id','Nome'], [6,"Alguem 2"])->buildQuery('insert');
    $dados = $sql
        ->tabela('testar')
        ->campos(['*'])
        //->limit(3,5)
        ->buildQuery('select');
    print_r($dados);
} catch(Exception $e) {
    print_r($e->getMessage());
    echo PHP_EOL.$e->getCode();
}