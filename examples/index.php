<?php
/**
 * Created by Nathan Feitoza.
 * User: dev01
 * Date: 30/04/18
 * Time: 08:51
 */
require '../vendor/autoload.php';

echo'<pre>';
try {

    //SQlite
    //$sql = new \Sigma\BuildQuery('sqlite',__DIR__.'/sqlite/teste.db',null,null, null,['dir_log' => __DIR__.'/logs/']);
    $sqlMysql = new \Sigma\BuildQuery('mysql','localhost','test','root','12345678');
    
    /*
    // Postgres
    $sql = \Sigma\BuildQuery::init('postgres','db','banco','usuario','senha');
    */


    // Firebird
    //$sql = \Sigma\BuildQuery::init('postgres','localhost','teste','postgres','123456');

    /*
    // Mysql
    $sql = \Sigma\BuildQuery::init('mysql','localhost','mysql','root','');
    */
    //$dados1 = $sql->tabela('Teste')->campos(['Id','Nome'], [6,"Alguem 2"])->buildQuery('insert');
    /*$dados = $sql
        ->tabela('teste')
        ->campos(['*'])
        //->limit(3,5)
        ->setGerarLog(true)
        ->buildQuery('select');*/
    
    //$dados = $sql->showTables();
    $dadosMysql = $sqlMysql->tabela('teste123')
    ->camposDdlCreate([
        'id' => [
            'type' => 'int',
            'options_field' => ['NOT NULL']
        ],
        'nome' => [
            'type' => 'TINYTEXT',
            'options_field' => ['NOT NULL']
        ]
        ], 'id')
        ->setEngineMysql('InnoDB')
        ->setGerarLog(true)
        ->createTable(); //

    //print_r($dados);
    print_r($dadosMysql);
} catch(Exception $e) {
    print_r($e->getMessage());
    echo PHP_EOL.$e->getCode();
}