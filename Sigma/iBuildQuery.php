<?php
/**
 * Created by Nathan Feitoza.
 * User: dev01
 * Date: 30/04/18
 * Time: 08:50
 */

 namespace Sigma;

interface iBuildQuery
{
    public function execSql($query, $parametros = false);
    public function tabela($tabela);
    public function campos($campos, $update = false);
    public function where($campo ,$operador,$valor);
    public function whereOr($campo, $operador,$valor);
    public function whereAnd($campo, $operador,$valor);
    public function whereComplex($campos, $operadores, $valores, $oper_logicos = false);
    public function leftJoin($tabela_join, $comparativo);
    public function rightJoin($tabela_join, $comparativo);
    public function innerJoin($tabela_join, $comparativo);
    public function fullOuterJoin($tabela_join, $comparativo);
    public function groupBy($tabela);
    public function groupByHaving($tabela, $clausula);
    public function orderBy($campo, $tipo);
    public function insertSelect($tabela, $campos);
    public function union($tipo = false);
    public function setMsgNaoEncontrado($msg);
    public function limit($limite, $offset = false);
    public function setGerarLog($gerar = true);
    public function setUsarExceptionNaoEncontrado($usar = true);
    public function setRetornoPersonalizado($retorno);
    public function buildQuery($tipo, $usando_union = false);
    public function getQueryMounted();
    public function showTables();
    public function camposDdlCreate(Array $campos, $primary_key = false);
    public function setEngineMysql($engine);
    public function setDefaultCharacter($caracter);
    public function setCollate($collate);
    public function setForeignKey($foreign_key_name, Array $refence, $onDelete = 'NO ACTION', $onUpdate = 'NO ACTION');
    public function createTable();
    public function dropTable();
    public function createView($nome_view);
    public function dropView($nome_view);
    public function getInformationDb($attribute = []);
}