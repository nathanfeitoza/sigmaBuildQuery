<?php
/**
 * Created by Nathan Feitoza.
 * User: dev01
 * Date: 30/04/18
 * Time: 08:50
 */
namespace Sigma;
interface iBuildQuery{
    public static function init($driver, $host, $dbname, $user, $pass, $opcoes=false);
    public function executarSQL($query, $parametros=false, $usar_transacao=false, $usar_exception_nao_encontrado=true);
    public function tabela($tabela);
    public function setRoolback();
    public function campos($campos,$update=false);
    public function where($campo,$operador,$valor);
    public function whereOr($campo,$operador,$valor);
    public function whereAnd($campo,$operador,$valor);
    public function whereComplex($campos, $operadores, $valores, $oper_logicos=false);
    public function leftJoin($tabela_join,$comparativo);
    public function rightJoin($tabela_join,$comparativo);
    public function innerJoin($tabela_join,$comparativo);
    public function fullOuterJoin($tabela_join,$comparativo);
    public function groupBy($tabela);
    public function groupByHaving($tabela,$clausula);
    public function orderBy($campo, $tipo);
    public function insertSelect($tabela,$campos);
    public function union($tipo=false);
    public function setTransactionUnitaria($pos=2, $fim=1);
    public function setMsgNaoEncontrado($msg);
    public function limit($limite, $offset=false);
    public function setGerarLog($gerar=true);
    public function setUsarExceptionNaoEncontrado($usar=true);
    public function setTransacaoMultipla();
    public function setCompletarTransacaoMultipla();
    public function getRetornarLinhasAfetadas();
    public function setRetornoPersonalizado($retorno);
    public function buildQuery($tipo,$usando_union=false);
}