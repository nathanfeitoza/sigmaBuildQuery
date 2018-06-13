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
    public function ExecSql($query, $parametros=false, $usar_transacao=false, $usar_exception_nao_encontrado=true);
    public function tabela($tabela);
    public function FazerRoolback();
    public function campos($campos,$update=false);
    public function where($campo,$operador,$valor);
    public function whereOr($campo,$operador,$valor);
    public function whereAnd($campo,$operador,$valor);
    public function whereComplex($campos, $operadores, $valores, $oper_logicos=false);
    public function leftjoin($tabela_join,$comparativo);
    public function rightjoin($tabela_join,$comparativo);
    public function innerjoin($tabela_join,$comparativo);
    public function fullouterjoin($tabela_join,$comparativo);
    public function groupby($tabela);
    public function groupbyHaving($tabela,$clausula);
    public function orderby($campo, $tipo);
    public function insertSelect($tabela,$campos);
    public function union($tipo=false);
    public function ComTransaction($pos=2, $fim=1);
    public function MsgNaoEncontrado($msg);
    public function limit($limite, $offset=false);
    public function GerarLog($gerar=true);
    public function UsarExceptionNaoEncontrado($usar=true);
    public function TransacaoMultipla();
    public function CompletarTransacaoMultipla();
    public function buildQuery($tipo,$usando_union=false);
}