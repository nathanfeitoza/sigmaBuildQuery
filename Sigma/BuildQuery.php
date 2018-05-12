<?php

/**
 * Created by Nathan Feitoza.
 * User: dev01
 * Date: 30/04/18
 * Time: 08:45
 */
namespace Sigma;

use PDO;
use Exception;
use json_encode;

class BuildQuery implements iBuildQuery
{
    private static $con;
    private static $driver;
    private static $dbname;
    private static $host;
    private static $user;
    private static $pass;
    private static $opcoes;

    private $method;
    private $util;
    private $table;
    private $table_in;
    private $string_build;
    private $campos_table;
    private $where = false;
    private $whereOr = false;
    private $whereAnd = false;
    private $whereComplex = false;
    private $rightjoin = false;
    private $innerjoin = false;
    private $leftjoin = false;
    private $fullouterjoin = false;
    private $groupby = false;
    private $orderby = false;
    private $valores_add = false;
    private $list_inter = false;
    private $valores_insert = [];
    private $valores_insert_bd = [];
    private $insertSelect = false;
    private $union = false;
    private $unionAll = false;
    private $comTransaction = false;
    private $nao_encontrado_per = false;
    private $limite = false;
    private $offset = false;
    private $gerar_log = false;
    private $exception_not_found = true;
    private $msg_erro;
    private $query_union = '';

    private function __construct(){}

    public static function init($driver, $host, $dbname, $user, $pass, $opcoes=false)
    {
        self::$driver = $driver;
        self::$host = $host;
        self::$dbname = $dbname;
        self::$user = $user;
        self::$pass = $pass;
        self::$opcoes = $opcoes;


        switch (self::$driver)
        {
            case "postgres":
                $db = "pgsql:";
                break;
            case "mysql":
                $db = "mysql:";
                break;
            case "firebird":
                $db = "firebird:";
                break;
			case "sqlite":
				$db = "sqlite:";
				break;
            default:
                $err = "Driver inválido";
                break;
        }
        if(!isset($err))
        {
            try {
                $dsn = self::$dbname != false ? $db . "host=" . self::$host . ";dbname=" . self::$dbname :  $db . "host=" . self::$host;
                if($db == "firebird:")
                {
                    $dsn = $db."dbname=".self::$host.':'.self::$dbname;
                } 
				elseif($db == "sqlite:")
				{
					$dsn = $db."".self::$host;
				}
                if(isset(self::$opcoes['port']))
                {
                    $porta = self::$opcoes['port'];
                    if(is_numeric($porta))
                    {
                        $dsn = $dsn.";port=".(int) $porta;
                    }
                }
                $pdo_case = PDO::CASE_NATURAL;
                if(isset(self::$opcoes['nome_campos']))
                {
                    $nome_campos = self::$opcoes['nome_campos'];
                    if(!empty($nome_campos))
                    {
                        switch ( strtolower( $nome_campos ) )
                        {
                            case 'mai':
                                $pdo_case = PDO::CASE_UPPER;
                                break;
                            case 'min':
                                $pdo_case = PDO::CASE_LOWER;
                                break;
                        }
                    }
                }
                $opcs = [
                    PDO::ATTR_CASE => $pdo_case,
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                ];

                if($db == "mysql:")
				{
				    $usar = isset(self::$opcoes['name_utf8_mysql']) ? self::$opcoes['name_utf8_mysql'] : false;
					if((boolean) $usar) {
                        $opcs[] = [PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES UTF8'];
                    }
				}

                self::$con = new PDO($dsn, self::$user, self::$pass, $opcs);
                return new self;
                //return self::$con;
            } catch (PDOException $e) {
                $msg = "ERRO DE CONEXÃO " . $e->getMessage();
                throw new Exception($msg, $e->getCode());
            }
        }
        else
        {
            $msg = "DRIVER INVÁLIDO";
            throw new Exception($msg, 001);
        }
    }


    public function ExecSql($query, $parametros=false, $usar_transacao=false, $usar_exception_nao_encontrado=true) // Metódo genérico para execuções de sql no banco de dados
    {
        $query_analize = explode(" ", $query);

        //print_r($parametros);
        if(is_array($query_analize) AND is_string($query))
        {

            $is_select = (strcmp(strtolower($query_analize[0]), "select") == 0) ? true : false;

            try
            {
                $pdo_obj = self::$con;
                if($usar_transacao)
                {
                    $pdo_obj->setAttribute(PDO::ATTR_AUTOCOMMIT, 0);
                    $pdo_obj->beginTransaction();

                }
				$not_enabled = ['firebird','sqlite'];
                if(!in_array(self::$driver, $not_enabled)) {
                    $data1 = $pdo_obj->prepare("SET NAMES 'UTF8'");
                }
                $data = $pdo_obj->prepare($query);

                if($parametros != false)
                {

                    if(is_array($parametros) AND count($parametros) != 0)
                    {
                        for($i = 0; $i < count($parametros); $i++)
                        {
                            $dados_query = $parametros[$i];
							if(is_integer($dados_query)) 
								$is_int = PDO::PARAM_INT;
							elseif(is_bool($dados_query))
								$is_int = PDO::PARAM_BOOL;
							elseif(is_null($dados_query))
								$is_int = PDO::PARAM_NULL;
							else
								$is_int = PDO::PARAM_STR;
                            $data->bindValue(($i + 1), $parametros[$i], $is_int);
                        }
                    }
                    else
                    {
                        var_dump($parametros);
                        throw new Exception("É necessário passar um array não nulo", 002);
                    }
                }
                if(isset($data1)) {
                    $data1->execute();
                }
                $data->execute();
                if($is_select)
                {
                    $tipo_retorno = PDO::FETCH_OBJ; // Retorna tipo objetos
                    if(isset(self::$opcoes['return_type']))
                    {
                        if((int) self::$opcoes['return_type'] == 2) {
                            $tipo_retorno = PDO::FETCH_NAMED; // Retorna Tipo array
                        }
                    }
                    $data_return = $data->fetchAll($tipo_retorno);

                    if(count($data_return) > 0)
                    {
                        if($usar_transacao)
                        {
                            $pdo_obj->commit();
                        }
                        $retorno_suc =  $data_return;
                    }
                    else
                    {
                        if($usar_transacao)
                        {
                            $pdo_obj->commit();
                        }
                        $retorno_err = ["Nada encontrado",710];
                        if($usar_exception_nao_encontrado)
                        {
                            //GERAR EXCEPTION
                            throw new Exception($retorno_err[0], $retorno_err[1]);
                        }
                        else
                        {
                            //NÃO GERAR EXCEPTION
                            $retorno_suc = $retorno_err[1];
                        }
                    }

                }
                else
                {
                    if($usar_transacao)
                    {
                        $pdo_obj->commit();
                    }
                    $retorno_suc = true;
                }

                return $retorno_suc;

            } catch (PDOException $e)
            {

                if($usar_transacao)
                {
                    $pdo_obj->rollBack();
                }
                $code = $e->getCode() == 710 ? $e->getCode() : 503;

                $retorno_err = [$e->getMessage().' - Query ExecSql: '.$query, $code];
                throw new Exception($retorno_err[0], $retorno_err[1]);
            }

        }
        else
        {
            throw new Exception("A Query passada não é válida", 003);
        }
    }

    protected function create($tipo, $table)
    {
        $this->table = $table;

        switch ($tipo)
        {
            case "select":
                $this->method = 'SELECT';
                $this->util = 'FROM';
                break;
            case "insert":
                $this->method = 'INSERT';
                $this->util = 'INTO';
                break;
            case "update":
                $this->method = 'UPDATE';
                $this->util = 'SET';
                break;
            case "delete":
                $this->method = 'DELETE';
                $this->util = 'FROM';
                break;
            default:
                $this->msg_erro = "Metódo Inválido";
        }

        return $this;
    }

    protected function WhereAdjust($tipo,$campo,$operador,$valor,$status_where = false)
    {
        if(!is_string($campo))
        {
            $msg_erro = "O campo precisa ser uma string";
        }
        elseif(!is_string($operador))
        {
            $msg_erro = "O operador precisa ser uma string";
        }
        else {
            if ($tipo == "where")
            {
                $where = "WHERE " . $campo ." ".$operador." ".$valor;
            }
            elseif($tipo == "or" OR $tipo == "and")
            {
                if($status_where == false)
                {
                    $msg_erro = "É necessário informar o parâmetro where antes";
                }
                else
                {
                    if ($tipo == "or") {
                        $whereOr[] = "OR " . $campo . " " . $operador . " " . $valor;
                    } else {
                        $whereAnd[] = "AND " . $campo . " " . $operador . " " . $valor;
                    }
                }
            }
            else
            {
                $msg_erro = "Parâmetro Where interno errado";
            }
        }

        if(isset($msg_erro))
        {
            return $this->msg_erro = $msg_erro;
        }
        elseif(isset($where))
        {
            return $this->where = $where;
        }
        elseif(isset($whereAnd))
        {
            return $this->whereAnd = $whereAnd;
        }
        elseif(isset($whereOr))
        {
            return $this->whereOr = $whereOr;
        }

    }
    // O comparativo dever ser feita em uma string da seguinte forma "a.tabela1 = b.tabela2"
    protected function Adjust_join($tipo, $tabela,$tabela_join,$comparativo)
    {
        if(!is_string($tipo))
        {
            $msg_erro = "O tipo precisa ser uma string";
        }
        elseif(!is_string($tabela))
        {
            $msg_erro = "A tabela precisa ser uma string";
        }
        elseif(!is_string($tabela_join))
        {
            $msg_erro = "A tabela do join precisa ser uma string";
        }
        elseif(!is_string($comparativo))
        {
            $msg_erro = "O comparativo precisa ser uma string";
        }
        else
        {

            if($tipo == "leftjoin")
            {
                $leftjoin = " LEFT JOIN ".$tabela_join." ON ".$comparativo." ";
            }
            elseif($tipo == "rightjoin")
            {
                $rightjoin = " RIGHT JOIN ".$tabela_join." ON ".$comparativo." ";
            }
            elseif($tipo == "innerjoin")
            {
                $innerjoin = " INNER JOIN ".$tabela_join." ON ".$comparativo." ";
            }
            elseif($tipo == "fullouterjoin")
            {
                $fullouterjoin = " FULL OUTER JOIN ".$tabela_join." ON ".$comparativo." ";
            }
            else
            {
                $msg_erro = "Método desconhecido, por favor, selecione: leftjoin, rightjoin, innerjoin ou fullouterjoin";
            }
        }

        if(isset($msg_erro))
        {
            return $this->msg_erro = $msg_erro;
        }
        elseif(isset($leftjoin))
        {
            return $this->leftjoin = $leftjoin;
        }
        elseif(isset($rightjoin))
        {
            return $this->rightjoin = $rightjoin;
        }
        elseif(isset($innerjoin))
        {
            return $this->innerjoin = $innerjoin;
        }
        elseif(isset($fullouterjoin))
        {
            return $this->fullouterjoin = $fullouterjoin;
        }
        else
        {
            return $this->msg_erro = "Erro interno na funcção que gera os joins";
        }

    }

    protected function AdjustgroupBy($tabela,$having=false)
    {
        if(!is_string($tabela))
        {
            $msg = "A tabela do groupby precisa ser uma string";
        }
        elseif($having != false &&  !is_string($having))
        {
            $msg = "O having precisa ser uma string";
        }
        else
        {
            $use_having = ($having != false) ? " HAVING ".$having : "";

            $group = " GROUP BY ".$tabela.$use_having;
        }

        if(isset($msg))
        {
            return $this->msg_erro = $msg;
        }
        else
        {
            return $this->groupby = $group;
        }


    }

    public function tabela($tabela)
    {
        $this->table_in = $tabela;
        return $this;
    }

    public function campos($campos,$update=false)
    {
        if(!is_array($campos))
        {
            throw new Exception("É necessário que os campos sejam passados em um array",853);
        }
        else {
            if ($update != false) {
                if (!is_array($update)) {
                    $this->msg_erro = "Os valores a serem inseridos precisam ser um array";
                } else {
                    if(count($update) == count($campos))
                    {
                        $interrogas = str_pad('', (count($update) * 2), "?,", STR_PAD_LEFT);
                        $interrogas = substr($interrogas, 0 , strlen($interrogas) - 1 );

                        $this->valores_insert = $update;
                        $this->valores_add = $update;
                        $this->list_inter = $interrogas;
                    }
                    else
                    {
                        throw new Exception("A quantidade de campos e de valores não coincidem -> ".json_encode( $campos ),109);
                    }
                }
            }
            $this->campos_table = $campos;
        }

        return $this;

    }

    public function where($campo,$operador,$valor)
    {
        $this->valores_insert[] = $valor;
        $this->WhereAdjust("where",$campo,$operador,'?');
        return $this;
    }

    public function whereOr($campo,$operador,$valor)
    {
        $this->valores_insert[] = $valor;
        $this->WhereAdjust("or",$campo,$operador,'?',$this->where);
        return $this;
    }

    public function whereAnd($campo,$operador,$valor)
    {
        $this->valores_insert[] = $valor;
        $this->WhereAdjust("and",$campo,$operador,'?',$this->where);
        return $this;
    }


    public function whereComplex($campos, $operadores, $valores, $oper_logicos=false)
    {
        if(!is_array($campos))
        {
            $this->msg_erro = "No where complexo os campos precisam ser um array";
        }
        elseif(!is_array($operadores))
        {
            $this->msg_erro = "No where complexo os operadores precisam ser um array";
        }
        elseif(!is_array($valores))
        {
            $this->msg_erro = "No where complexo os valores precisam ser um array";
        }
        elseif(!is_array(@$oper_logicos))
        {
            $this->msg_erro = "No where complexo os operadores lógicos precisam ser um array";
        }
        else
        {
            if($this->where == false)
            {
                $this->msg_erro = "Para utilizar o where complexo, é necessário instanciar o where primeiro";
            }
            else
            {
                $cont_campos = count($campos);
                $cont_operadores = count($operadores);
                $cont_valores = count($valores);
                $cont_logicos = count($oper_logicos);

                $tudo = array($cont_campos,$cont_operadores,$cont_valores,$cont_logicos);

                foreach ($tudo as $key => $comp)
                {
                    if($tudo[0] != $comp)
                    {
                        $this->msg_erro = "As quantidade de valores não são equivalentes, por favor corrija";
                    }
                }

                if(!isset($this->msg_erro))
                {
                    $s = '';

                    for($i = 0; $i < count($campos); $i++)
                    {
                        if($i == 0)
                        {
                            $this->valores_insert[] = $valores[0];
                            $s .= $oper_logicos[0]." (".$campos[0]." ".$operadores[0]." ? ";
                        }
                        elseif($i == count($campos) - 1)
                        {
                            $this->valores_insert[] = $valores[$i];
                            $s .=	$oper_logicos[$i]." ".$campos[$i]." ".$operadores[$i]." ? )";
                            $this->whereComplex[] = $s;
                        }
                        else
                        {
                            $this->valores_insert[] = $valores[$i];
                            $s .= $oper_logicos[$i]." ".$campos[$i]." ".$operadores[$i]." ? ";
                        }
                    }
                }
            }
        }
        return $this;

    }

    public function leftjoin($tabela_join,$comparativo)
    {
        $this->Adjust_join("leftjoin", $this->table_in,$tabela_join,$comparativo);
        return $this;
    }

    public function rightjoin($tabela_join,$comparativo)
    {
        $this->Adjust_join("rightjoin", $this->table_in,$tabela_join,$comparativo);
        return $this;
    }

    public function innerjoin($tabela_join,$comparativo)
    {
        $this->Adjust_join("innerjoin", $this->table_in,$tabela_join,$comparativo);
        return $this;
    }

    public function fullouterjoin($tabela_join,$comparativo)
    {
        $this->Adjust_join("fullouterjoin", $this->table_in,$tabela_join,$comparativo);
        return $this;
    }

    public function groupby($tabela)
    {
        $this->AdjustgroupBy($tabela);
        return $this;
    }

    public function groupbyHaving($tabela,$clausula)
    {
        $this->AdjustgroupBy($tabela,$clausula);
        return $this;
    }

    public function orderby($campo, $tipo)
    {
        if(!is_string($campo))
        {
            $this->msg_erro = "O campo precisa ser uma string";
        }
        elseif(!is_string($tipo) AND (strtoupper($tipo) == "ASC" OR strtoupper($tipo) == "DESC"))
        {
            $this->msg_erro = "O tipo precisa ser uma string, sendo ou ASC ou DESC ";
        }
        else
        {
            if($this->orderby == false)
            {
                $this->orderby = "ORDER BY " . $campo . " " . $tipo;
            }
            else
            {
                $this->orderby = $this->orderby.', '.$campo.' '.$tipo;
            }
        }

        return $this;
    }

    public function insertSelect($tabela,$campos)
    {
        if(!is_array($campos))
        {
            throw new Exception("Os campos do insertSelect precisam ser passados em um array");
        }
        elseif(!is_string($tabela))
        {
            throw new Exception("A tabela do insertSelect precisa ser uma String");
        }
        else
        {
            $campos_usar = implode(",",$campos);
            $this->insertSelect = "SELECT ".$campos_usar." FROM ".$tabela;
        }
        return $this;
    }

    public function union($tipo=false)
    {
        $virar_false = ['table',
            'table_in',
            'string_build',
            'campos_table',
            'where',
            'whereOr',
            'whereAnd',
            'whereComplex',
            'rightjoin',
            'innerjoin',
            'leftjoin',
            'fullouterjoin',
            'groupby',
            'orderby',
            'valores_add',
            'list_inter',
            'insertSelect',
            'union',
            'unionAll',
            'valores_insert',
            'ComTransaction',
            'limite',
            'offset'];

        foreach ($this as $key => $value)
        {
            if(in_array($key, $virar_false))
            {
                $this->$key = false;
            }
        }

        switch( strtolower($tipo) )
        {
            case 'all':
                $this->unionAll = ' UNION ALL ';
                break;
            case 'union':
                $this->union = ' UNION ';
                break;
            case 'unionall':
                $this->unionAll = ' UNION ALL ';
                break;
            default:
                $this->union = ' UNION ';
        }

        return $this;
    }

    public function ComTransaction()
    {
        $this->comTransaction = true;
        return $this;
    }

    public function MsgNaoEncontrado($msg)
    {
        $this->nao_encontrado_per = $msg;

        return $this;
    }

    public function limit($limite, $offset=false)
    {
        if($limite != false)
        {
            $this->limite = $limite;
            $this->offset = $offset;

            $this->valores_insert[] = (int) $limite;
        }

        if($offset != false)
        {
            $this->valores_insert[] = (int) $offset;
        }

        return $this;
    }

    public function GerarLog($gerar=true)
    {
        $this->gerar_log = $gerar;
        return $this;
    }

    public function UsarExceptionNaoEncontrado($usar=true)
    {
        $this->exception_not_found = $usar;
        return $this;
    }

    public function buildQuery($tipo,$usando_union=false)
    {

        $this->create($tipo,$this->table_in);

        if(isset($this->campos_table) AND !is_array($this->campos_table))
        {
            $this->msg_erro = "Os campos não são um array";
            $code_error = 005;

        }
        elseif(!is_string($this->table))
        {
            $this->msg_erro = "A tabela precisa ser uma string";
            $code_error = 006;
        }

        if(isset($this->msg_erro))
        {
            $msg = (isset($msg)) ? $msg : $this->msg_erro;
            $code_erro_return = isset($code_error) ? $code_error : 405;
            throw new Exception($msg, $code_erro_return);

        }

        $campos_usar = (isset($this->campos_table)) ? implode(",",$this->campos_table) : "*";
        $where = ($this->where != false) ? " ".$this->where : "";
        $whereOr = ($this->whereOr != false ) ? " ".implode(" ",$this->whereOr) : "";
        $whereAnd = ($this->whereAnd != false ) ? " ".implode(" ",$this->whereAnd) : "";
        $whereComplex = ($this->whereComplex != false) ? " ".implode(" ",$this->whereComplex) : "";
        $orderby = ($this->orderby != false) ? " ".$this->orderby : "";
        $union = ($this->union != false && $this->unionAll == false) ? $this->union : '';
        $unionAll = ($this->union == false && $this->unionAll != false) ? $this->unionAll : '';
        $msg_nao_encontrado = ($this->nao_encontrado_per != false) ? $this->nao_encontrado_per : 'Nada Encontrado';

        // Joins
        $leftjoin = ($this->leftjoin != false) ? $this->leftjoin : "";
        $rightjoin = ($this->rightjoin != false) ? $this->rightjoin : "";
        $innerjoin = ($this->innerjoin != false) ? $this->innerjoin : "";
        $fullouterjoin = ($this->fullouterjoin != false) ? $this->fullouterjoin : "";
        // Fim Joins

        $groupby = ($this->groupby != false) ? $this->groupby." " : "";
        $limite = ((string) $this->limite != false) ? ' LIMIT ?' : "";
        $limite = ( (string) $this->offset != false) ? $limite.' OFFSET ?' : $limite;


        if($this->method == "SELECT") {

            switch (self::$driver)
            {
                case "firebird":
                    $limitar = ((string) $this->limite != false) ? ' FIRST ? ' : "";
                    $limitar = ( (string) $this->offset != false) ? $limitar.' SKIP ? ' : $limitar;

                    $campos_usar = (strlen($limitar) > 0) ? $limitar.$campos_usar : $campos_usar;
                    $limite = "";
                    break;
                case "mysql":
                    $limite = ( (string) $this->offset != false) ? ' LIMIT ?,?' : $limite;
                    break;
            }

            $string_build = $this->method . " "
                . $campos_usar . " "
                . $this->util . " "
                . $this->table
                . $leftjoin
                . $rightjoin
                . $innerjoin
                . $fullouterjoin
                . $where
                . $whereOr
                . $whereAnd
                . $whereComplex
                . $groupby
                . $orderby
                . $limite;

        }
        elseif ($this->method == "INSERT")
        {
            if($this->list_inter == false AND $this->insertSelect == false)
            {
                throw new Exception("É ncessário passar os valores dos campos",007);

            }
            else
            {
                switch ($this->insertSelect)
                {
                    case false:
                        $suf_insert = "VALUES ("
                            . $this->list_inter . ")";
                        break;
                    default:
                        $suf_insert = $this->insertSelect
                            . $leftjoin
                            . $rightjoin
                            . $innerjoin
                            . $fullouterjoin
                            . $where
                            . $whereOr
                            . $whereAnd
                            . $whereComplex
                            . $groupby
                            . $orderby;
                        break;
                }

                $string_build = $this->method . " "
                    . $this->util . " "
                    . $this->table . "("
                    .$campos_usar.") "
                    .$suf_insert;
            }
        }
        elseif($this->method == "UPDATE")
        {
            if($campos_usar != '*')
            {
                for ($i = 0; $i < count($this->campos_table); $i++)
                {
                    $campos_use = $this->campos_table;
                    $campos_atualizar[] = $campos_use[$i] . ' = ? ';
                }

                $campos_usar = implode(',',$campos_atualizar);

                $string_build = $this->method . " "
                    . $this->table . " "
                    . $this->util . " "
                    . $campos_usar . ""
                    . $where
                    . $whereOr
                    . $whereAnd
                    . $whereComplex;
            }
            else
            {
                throw new Exception('Layout incorreto para o método UPDATE ',107);
            }
        }
        elseif($this->method == "DELETE")
        {
            $string_build = $this->method . " "
                . $this->util . " "
                . $this->table . ""
                . $where
                . $whereOr
                . $whereAnd
                . $whereComplex
                . $groupby;
        }
        else
        {
            throw new Exception("Metódo desconhecido", 108);

        }



        $this->query_union .= $union
            .$unionAll
            .$string_build;
        $this->valores_insert_bd[] = $this->valores_insert;

        if(!$usando_union)
        {
            try
            {

                $count_insert_bd = count($this->valores_insert_bd);
                if($count_insert_bd > 0)
                {
                    $count_insert = 1;
                    $dados_insert_query = [];
                    for ($i = 0; $i < $count_insert_bd; $i++)
                    {
                        $matriz_insert = $this->valores_insert_bd[$i];

                        for ($j = 0; $j < count($matriz_insert); $j++)
                        {
                            $elemento_insert = $matriz_insert[$j];
                            //if(strlen($elemento_insert) > 0)
                            //{
                            $dados_insert_query[] = $elemento_insert;
                            //}

                        }
                    }

                    $dados_insert_query = is_array($dados_insert_query) ? $dados_insert_query : [$dados_insert_query];

                    $retorno = $this->ExecSql($this->query_union, $dados_insert_query,$this->comTransaction, $this->exception_not_found);
                }
                else
                {
                    $retorno = $this->ExecSql($this->query_union, false,$this->comTransaction, $this->exception_not_found);

                }

            } catch(Exception $e)
            {
                $valores_query = json_encode($dados_insert_query);
                $msg_error = ($e->getCode() == 710) ? $msg_nao_encontrado.';'.$this->gerar_log.';'.$this->query_union . ' Valores: '.$valores_query : "Erro no banco de dados: ".$e->getMessage().'. Query: '.$this->query_union." -> valores_query => ".json_encode($dados_insert_query);
                throw new  Exception($msg_error, (int) $e->getCode());
            }

        }
        else
        {
            $retorno = $this;
        }

        if(!$usando_union)
        {
            if($this->gerar_log)
            {
                $valores_query = json_encode($dados_insert_query);
                $retorno = [$retorno, "query_sql" => $this->query_union." -> valores_query => ".$valores_query];
            }
            $virar_false = [
                'table',
                'table_in',
                'string_build',
                'campos_table',
                'where',
                'whereOr',
                'whereAnd',
                'whereComplex',
                'rightjoin',
                'innerjoin',
                'leftjoin',
                'fullouterjoin',
                'groupby',
                'orderby',
                'valores_add',
                'list_inter',
                'insertSelect',
                'union',
                'unionAll',
                'ComTransaction',
                'limite',
                'offset',
                'query_union',
                'valores_insert_bd' => [],
                'valores_insert' => [],
                'exception_not_found' => true];
            $novo_array = [];
            foreach ($this as $key => $value)
            {
                if(in_array($key, $virar_false))
                {
                    $valor = false;
                    if(is_array($key)) {
                        $key = $key[0];
                        $valor = $key[1];
                    }
                    $this->$key = $valor;

                }
            }
            $this->query_union = '';
        }

        return $retorno;
    }

}