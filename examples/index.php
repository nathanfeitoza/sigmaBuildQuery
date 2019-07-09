<?php
/**
 * Created by Nathan Feitoza.
 * User: dev01
 * Date: 30/04/18
 * Time: 08:51
 */
require '../vendor/autoload.php';

try {

    //SQlite
    $sql = new \Sigma\BuildQuery('sqlite',__DIR__.'/sqlite/teste.db',null,null, null,['dir_log' => __DIR__.'/logs/']);
    $sqlMysql = new \Sigma\BuildQuery('mysql','localhost','test','root','12345678', ['dir_log' => __DIR__.'/logs/']);
    
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
        ->buildQuery('select', true)->createView('view_teste'); */

    //$dados = $sql->dropView('view_teste');

    $dados = $sql->getInformationDb();
    //$dadosMysql = $sqlMysql->getInformationDb();

    
    //$dados = $sql->showTables();
    $dadosMysql = $sqlMysql->tabela('testando_teste')
    ->camposDdlCreate([
        'id' => [
            'type' => 'int',
            'options_field' => ['NOT NULL']
        ],
        'nome' => [
            'type' => 'TEXT',
            'options_field' => ['NOT NULL']
        ]
        ], 'id')
        ->setEngineMysql('InnoDB')
        ->campos(['id'])
        ->setForeignKey('teste_fk', ['tabela' => 'teste123', 'campos' => ['id']])
        ->setGerarLog(true)
        ->createTable(); //

    print_r($dados);
    print_r($dadosMysql);
} catch(\Sigma\BuildQueryException $e) {
    //print_r($e->getMessage());
    echo PHP_EOL.$e->getMessage();
}