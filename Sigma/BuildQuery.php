<?php
/**
 * Created by Nathan Feitoza.
 * User: nathan
 * Date: 11/09/17
 * Time: 23:55
 * 
 * This class was used for generate the codes sql to execute by PDO easy
 * This class use Exception to trigger erros what the best method to break the critical execution
 * 
 */

namespace Sigma;

use PDO;
use json_encode;

class BuildQuery implements iBuildQuery
{
    /**
     * Declaration of variables connection
     *
     * @var [type]
     */
    private $con;
    private $driver;
    private $dbname;
    private $host;
    private $user;
    private $pass;
    private $opcoes;


    /**
     * Declaration of variables is storages for data
     *
     * @var [type]
     */
    private $method;
    private $util;
    private $table;
    private $table_in;
    private $string_build;
    private $campos_table, $campos_ddl;
    private $where = false;
    private $whereOr = false;
    private $whereAnd = false;
    private $whereComplex = false;
    private $rightJoin = false;
    private $innerJoin = false;
    private $leftJoin = false;
    private $fullOuterJoin = false;
    private $groupBy = false;
    private $orderBy = false;
    private $valores_add = false;
    private $list_inter = false;
    private $valores_insert = [];
    private $valores_insert_bd = [];
    private $valores_insert_final = [];
    private $insertSelect = false;
    private $union = false;
    private $unionAll = false;
    private $comTransaction = false;
    private $fazer_rollback = false;
    private $posTransaction = 'a';
    private $fimTransaction = 'b';
    private $iniciar_transacao = false;
    private $nao_encontrado_per = false;
    private $limite = false;
    private $offset = false;
    private $gerar_log = false;
    private $exception_not_found = true;
    private $retornar_false_not_found = false;
    private $msg_erro = false;
    private $query_union = '';
    private $retorno_personalizado = false;
    private $linhas_afetadas = 0;
    private $transacao_multipla = false, $pos_multipla = 0, $finalizar_multipla = false;
    protected $pdo_obj_usando = false, $contarLinhasAfetadas = false, $eventos_gravar = false, $eventos_retornar = false;
    protected $pdo_padrao = false, $gravar_log_complexo = false, $dados_select_transacao;
    private $logger = false, $file_handler = false;
    private $count_afetadas_insert = 0, $engineMysql, $characterMysql, $collateMysql, $query_mounted, $foreign_key;
    public $gravarsetLogComplexo;

    /**
     * Construct class setting the values for connect in database
     *
     * @param [type] $driver
     * @param [type] $host
     * @param [type] $dbname
     * @param [type] $user
     * @param [type] $pass
     * @param boolean $opcoes
     */
    public function __construct($driver, $host, $dbname, $user, $pass, $opcoes=false)
    {  
        $this->driver = $driver;
        $this->host = $host;
        $this->dbname = $dbname;
        $this->user = $user;
        $this->pass = $pass;
        $this->opcoes = $opcoes;
        $this->logger = new \Monolog\Logger('BDLOG');
        $local_logs = isset($opcoes['dir_log']) ? $opcoes['dir_log'] : __DIR__.DIRECTORY_SEPARATOR;
        $nome_arquivo = date('d-m-Y');
        $local_logs .= $nome_arquivo.'_BuildQuery.log';
        $this->file_handler = new \Monolog\Handler\StreamHandler($local_logs);

        switch ($this->driver) {
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

        if(!isset($err)) {
            try {
                $dsn = $this->dbname != false ? $db . "host=" . $this->host . ";dbname=" . $this->dbname :  $db . "host=" . $this->host;
                if($db == "firebird:") {
                    $dsn = $db."dbname=".$this->host.':'.$this->dbname;
                }
                elseif($db == "sqlite:") {
                    $dsn = $db."".$this->host;
                }
                if(isset($this->opcoes['port'])) {
                    $porta = $this->opcoes['port'];
                    if(is_numeric($porta)) {
                        $dsn = $dsn.";port=".(int) $porta;
                    }
                }
                $pdo_case = PDO::CASE_NATURAL;
                if(isset($this->opcoes['nome_campos'])) {
                    $nome_campos = $this->opcoes['nome_campos'];
                    if(!empty($nome_campos)) {
                        switch ( strtolower( $nome_campos ) ) {
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

                if($db == "mysql:") {
                    $usar = isset($this->opcoes['name_utf8_mysql']) ? $this->opcoes['name_utf8_mysql'] : false;
                    if((boolean) $usar) {
                        $opcs[] = [PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES UTF8'];
                    }
                }

                $this->con = new PDO($dsn, $this->user, $this->pass, $opcs);
                return $this;
                //return $this->con;
            } catch (PDOException $e) {
                $msg = "ERRO DE CONEXÃO " . $e->getMessage();
                throw new BuildQueryException($msg, $e->getCode());
            }
        } else {
            $msg = "DRIVER INVÁLIDO";
            throw new BuildQueryException($msg, 001);
        }
    }

    /**
     * Set value Var
     *
     * @param [type] $var
     * @param [type] $value
     * @return void
     */
    protected function _set($var, $value) 
    {
        if(isset($this->$var)) {
            $this->$var = $value;
        }
    }

    /**
     * Get value var
     *
     * @param [type] $var
     * @return void
     */
    public function _get($var)
    {
        if(isset($this->$var)) {
            return $this->$var;
        }
    }

    /**
     * This Method set the log of class using Monolog
     *
     * @param [type] $msg
     * @param [type] $type
     * @return void
     */
    protected function setLog($msg, $type)
    {
        $this->logger->pushHandler($this->file_handler);
        $configs_db = [ $this->driver,
            $this->host,
            $this->dbname,
            $this->user];
        $msg .= ' - DADOS DB: '.json_encode($configs_db);
        switch( strtolower( $type ) ) {
            case 'error':
                $this->logger->addError($msg);
                break;
            case 'alert':
                $this->logger->addAlert($msg);
                break;
            case 'critical':
                $this->logger->addCritical($msg);
                break;
            case 'debug':
                $this->logger->addDebug($msg);
                break;
            case 'emergency':
                $this->logger->addEmergency($msg);
                break;
            case 'notice':
                $this->logger->addNotice($msg);
                break;
            case 'record':
                $this->logger->addRecord($msg);
                break;
            case 'warning':
                $this->logger->addWarning($msg);
                break;
            default:
                $this->logger->addInfo($msg);
        }
    }

    private final function getMarcador()
    {
        $marcador = "'";
        if($this->driver == "mysql") $marcador = '`';

        return $marcador;
    }

    /**
     * This method setPdo for reuse in execution, for example, transaction
     *
     * @param [type] $pdo
     * @return void
     */
    protected function setPDO($pdo)
    {
        $this->pdo_padrao = $pdo;
    }

    /**
     * Get the PDO instance
     *
     * @return void
     */
    protected function pdo()
    {
        $this->pdo_padrao = !$this->pdo_padrao ? $this->con : $this->pdo_padrao;
        return $this->pdo_padrao;
    }

    /**
     * Init transaction
     *
     * @return void
     */
    public function iniciarTransacao()
    {
        $this->iniciar_transacao = true;
        return $this;
    }

    /**
     * Rollback the transaction. Automatically executed in Exception
     *
     * @return void
     */
    public function rollback()
    {
        if($this->pdo()->inTransaction()) $this->pdo()->rollback();
    }

    /**
     * Commit the transaction
     *
     * @return void
     */
    public function commit()
    {
        if($this->pdo()->inTransaction()) $this->pdo()->commit();
    }

    /**
     * This method exec sql commands
     *
     * @param [type] $query
     * @param boolean $parametros
     * @return void
     */
    public function execSql($query, $parametros=false)
    {
        $exception_nao_encontrado = $this->exception_not_found;
        $retornar_false_nao_encontrado = $this->retornar_false_not_found;
        $msg_nao_encontrado = $this->nao_encontrado_per;

        if(!is_string($query)) throw new BuildQueryException('A querya informada não é uma string', 4578);

        $query_analize = explode(" ", $query);
        $tipo = strtolower($query_analize[0]);

        $pdo_obj = $this->pdo();


        if ($this->iniciar_transacao and !$pdo_obj->inTransaction()) {
            if (strtolower($this->driver) == "firebird") $pdo_obj->setAttribute(PDO::ATTR_AUTOCOMMIT, 0);

            $pdo_obj->beginTransaction();
            $this->setPDO($pdo_obj);
        }

        try {
            $not_enabled = ['firebird', 'sqlite'];
            if (!in_array($this->driver, $not_enabled)) {
                $data1 = $pdo_obj->prepare("SET NAMES 'UTF8'");
            }

            $data = $pdo_obj->prepare($query);

            if ($parametros != false) {
                if (!(is_array($parametros) AND count($parametros) != 0)) {
                    $msg = 'É necessário passar um array não nulo';
                    $this->setLog($msg, 'error');
                    throw new BuildQueryException($msg, 002);
                }

                for ($i = 0; $i < count($parametros); $i++) {
                    $dados_query = $parametros[$i];
                    if (is_integer($dados_query))
                        $is_int = PDO::PARAM_INT;
                    elseif (is_bool($dados_query))
                        $is_int = PDO::PARAM_BOOL;
                    elseif (is_null($dados_query))
                        $is_int = PDO::PARAM_NULL;
                    else
                        $is_int = PDO::PARAM_STR;
                    
                    $data->bindValue(($i + 1), $parametros[$i], $is_int);
                }

            }

            if (isset($data1)) $data1->execute();
            
            $exec = $data->execute();

            $enable_fetch = in_array($tipo, ['select', 'show']);

            if ($enable_fetch) {
                $tipo_retorno = PDO::FETCH_OBJ; // Retorna tipo objetos
                
                if (isset($this->opcoes['return_type'])) {
                    if ((int)$this->opcoes['return_type'] == 2) {
                        $tipo_retorno = PDO::FETCH_NAMED; // Retorna Tipo array
                    }
                }

                $data_return = $data->fetchAll($tipo_retorno);

                if (count($data_return) == 0) {
                    $retorno_err = ["Nada encontrado", 710];
                    if ($exception_nao_encontrado) {
                        //GERAR EXCEPTION
                        $msgUsuario = !$msg_nao_encontrado ? $retorno_err[0] : $msg_nao_encontrado;
                        $this->setLog($retorno_err[0] . ' - QUERY: ' . $query . ' - VALORES: ' . json_encode($parametros), 'error');
                        $erro = [$retorno_err[0], $retorno_err[1], 404, $msgUsuario];
                    }
                    $this->setLog($retorno_err[1] . ' - QUERY: ' . $query . ' - VALORES: ' . json_encode($parametros), 'error');
                    //NÃO GERAR EXCEPTION
                    $retorno_suc = !$retornar_false_nao_encontrado ? $retorno_err[1] : false;
                } else {
                    $retorno_suc = $data_return;
                }
            }

            if (!$enable_fetch) {

                $usar = $data->rowCount();

                if (strcmp(strtolower($query_analize[0]), "insert") == 0) {
                    $this->count_afetadas_insert += $data->rowCount();
                    $usar = $this->count_afetadas_insert;
                }

                $this->setLinhasAfetadas($usar);

                $retorno_suc = $exec;
            }

        } catch (\Exception $e) {
            $this->rollback();

            $this->count_afetadas_insert = 0;
            $code = $e->getCode() == 710 ? $e->getCode() : 503;
            $msg = $e->getMessage().' - Query execSql: '.$query;
            $retorno_err = [$msg, $code];
            $this->_set('msg_erro', $retorno_err);
            $this->setLog($msg.' - VALORES: '.json_encode($parametros), 'critical');

            throw new BuildQueryException($retorno_err[0], $retorno_err[1]);
        } finally {

            if($this->gravar_log_complexo == false) {
                $this->setLogComplexo($query, $parametros);
                $this->gravar_log_complexo = true;
            }

            $msg_log = isset($retorno_suc) ? $retorno_suc : $this->_get('msg_erro');

            if ($this->gerar_log) {
                $this->setLog(json_encode([$msg_log, "query_sql" => $query, "valores_query" => $parametros]), 'info');
            }
        }

        if (isset($erro)) {
            $erro_msg = $erro[0];
            $cod_erro = $erro[1];

            if($cod_erro == 710) {
                $erro_msg = 'Nada encontrado na query: '.$query.' com os valores = '.json_encode($parametros);
            }

            throw new BuildQueryException($erro_msg, $cod_erro);
        }

        return $retorno_suc;

    }

    /**
     * The method clear values for reuse in union or multiples calls to object instance class in code flow
     *
     * @param boolean $union
     * @return void
     */
    protected function limparValores($union=false)
    {
        $array_valores = [
            'table',
            'table_in',
            'string_build',
            'campos_table',
            'campos_ddl',
            'query_mounted',
            'engineMysql',
            'characterMysql',
            'collateMysql',
            'foreign_key',
            'where',
            'whereOr',
            'whereAnd',
            'whereComplex',
            'rightJoin',
            'innerJoin',
            'leftJoin',
            'fullOuterJoin',
            'groupBy',
            'orderBy',
            'valores_add',
            'list_inter',
            'insertSelect',
            'union',
            'unionAll',
            'comTransaction',
            'fazer_rollback',
            'posTransaction'=> ['a'],
            'fimTransaction'=>['b'],
            'limite',
            'offset',
            'retorno_personalizado',
            'query_union',
            'valores_insert_bd' => [[]],
            'valores_insert' => [[]],
            'valores_insert_final' => [[]],
            'exception_not_found' => [true]];

        if($union != false) {
            $array_valores = ['table',
                'table_in',
                'string_build',
                'retorno_personalizado',
                'campos_table',
                'campos_ddl',
                'query_mounted',
                'engineMysql',
                'characterMysql',
                'collateMysql',
                'foreign_key',
                'where',
                'whereOr',
                'whereAnd',
                'whereComplex',
                'rightJoin',
                'innerJoin',
                'leftJoin',
                'fullOuterJoin',
                'groupBy',
                'orderBy',
                'valores_add',
                'list_inter',
                'insertSelect',
                'union',
                'unionAll',
                'valores_insert' => [[]],
                'valores_insert_final' => [[]],
                'comTransaction',
                'fazer_rollback',
                'posTransaction'=> ['a'],
                'fimTransaction'=>['b'],
                'limite',
                'offset'];
        }
        $novo_array = [];

        if($this->finalizar_multipla) {
            $this->pos_multipla = 0;
            $this->transacao_multipla = false;
            $this->finalizar_multipla = false;
        }

        if($this->posTransaction == $this->fimTransaction) {
            $this->pdo_obj_usando = false;
        }

        foreach ($this as $key => $value) {
            if(in_array($key, $array_valores) || array_key_exists($key, $array_valores)) {
                $valor = false;
                if(array_key_exists($key, $array_valores)) {
                    $valor = $array_valores[$key][0];
                }
                $this->$key = $valor;
            }
        }

        return $this;
    }

    /**
     * Create the head for command DML
     *
     * @param [type] $tipo
     * @param [type] $table
     * @return void
     */
    protected function create($tipo, $table)
    {
        $this->table = $table;

        switch ($tipo) {
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

    /**
     * Adjust Wheres for all databases
     *
     * @param [type] $tipo
     * @param [type] $campo
     * @param [type] $operador_comparacao
     * @param [type] $valor
     * @param boolean $status_where
     * @return void
     */
    protected function ajustarWhere($tipo,$campo,$operador_comparacao,$valor,$status_where = false)
    {
        if(!is_string($campo)) {
            $msg_erro = "O campo precisa ser uma string";
        } elseif(!is_string($operador_comparacao)) {
            $msg_erro = "O operador precisa ser uma string";
        } else {
            if ($tipo == "where") {
                $where = "WHERE " . $campo ." ".$operador_comparacao." ".$valor;
            }
            elseif($tipo == "or" OR $tipo == "and") {
                if($status_where == false) {
                    $msg_erro = "É necessário informar o parâmetro where antes";
                } else {
                    if ($tipo == "or") {
                        $whereOr = "OR " . $campo . " " . $operador_comparacao . " " . $valor;
                    } else {
                        $whereAnd = "AND " . $campo . " " . $operador_comparacao . " " . $valor;
                    }
                }
            } else {
                $msg_erro = "Parâmetro Where interno errado";
            }
        }

        if(isset($msg_erro)) {
            return $this->msg_erro = $msg_erro;
        } elseif(isset($where)) {
            return $this->where = $where;
        } elseif(isset($whereAnd)) {
            return $this->whereAnd[] = $whereAnd;
        } elseif(isset($whereOr)) {
            return $this->whereOr[] = $whereOr;
        }

    }

    /**
     * Adjust join to all databases
     * The comparison should be made in a string as follows "a.table1 = b.table2"
     * 
     * @param [type] $tipo
     * @param [type] $tabela
     * @param [type] $tabela_join
     * @param [type] $comparativo
     * @return void
     */
    protected function ajustarJoin($tipo, $tabela,$tabela_join,$comparativo)
    {
        if(!is_string($tipo)) {
            $msg_erro = "O tipo precisa ser uma string";
        } /*elseif(!is_string($tabela)) { // Removido para ter a instância da classe reutilizável
            $msg_erro = "A tabela precisa ser uma string";
        }*/ elseif(!is_string($tabela_join)) {
            $msg_erro = "A tabela do join precisa ser uma string";
        } elseif(!is_string($comparativo)) {
            $msg_erro = "O comparativo precisa ser uma string";
        } else {

            if($tipo == "leftJoin") {
                $leftjoin = " LEFT JOIN ".$tabela_join." ON ".$comparativo." ";
            } elseif($tipo == "rightJoin") {
                $rightjoin = " RIGHT JOIN ".$tabela_join." ON ".$comparativo." ";
            }
            elseif($tipo == "innerJoin") {
                $innerjoin = " INNER JOIN ".$tabela_join." ON ".$comparativo." ";
            } elseif($tipo == "fullOuterJoin") {
                $fullouterjoin = " FULL OUTER JOIN ".$tabela_join." ON ".$comparativo." ";
            } else {
                $msg_erro = "Método desconhecido, por favor, selecione: leftJoin, rightJoin, innerJoin ou fullOuterJoin";
            }
        }

        if(isset($msg_erro)) {
            return $this->msg_erro = $msg_erro;
        }
        elseif(isset($leftjoin)) {
            return $this->leftJoin[] = $leftjoin;
        }
        elseif(isset($rightjoin)) {
            return $this->rightJoin[] = $rightjoin;
        }
        elseif(isset($innerjoin)) {
            return $this->innerJoin[] = $innerjoin;
        }
        elseif(isset($fullouterjoin)) {
            return $this->fullOuterJoin[] = $fullouterjoin;
        } else {
            return $this->msg_erro = "Erro interno na funcção que gera os joins";
        }

    }
    
    /**
     * Adjust GroupBy for all databases
     *
     * @param [type] $tabela
     * @param boolean $having
     * @return void
     */
    protected function ajustarGroupBy($campos,$having=false)
    {
        if(!is_string($campos)) {
            $msg = "A tabela do groupBy precisa ser uma string";
        } elseif($having != false &&  !is_string($having)) {
            $msg = "O having precisa ser uma string";
        } else {
            $use_having = ($having != false) ? " HAVING ".$having : "";

            $group = " GROUP BY ".$campos.$use_having;
        }

        if(isset($msg)) {
            return $this->msg_erro = $msg;
        } else {
            return $this->groupBy = $group;
        }


    }

    /**
     * Set the table using in execution
     *
     * @param [type] $tabela
     * @return void
     */
    public function tabela($tabela)
    {
        $this->table_in = $tabela;
        return $this;
    }

    /**
     * Set the fields using in DML
     *
     * @param [type] $campos
     * @param boolean $update
     * @return void
     */
    public function campos($campos,$update=false)
    {
        if(!is_array($campos)) {
            throw new BuildQueryException("É necessário que os campos sejam passados em um array",853);
        } else {
            if ($update != false) {
                if (!is_array($update)) {
                    $this->msg_erro = "Os valores a serem inseridos precisam ser um array";
                } else {
                    if(count($update) == count($campos)) {
                        $interrogas = str_pad('', (count($update) * 2), "?,", STR_PAD_LEFT);
                        $interrogas = substr($interrogas, 0 , strlen($interrogas) - 1 );

                        $this->valores_insert = $update;
                        $this->valores_add = $update;
                        $this->list_inter = $interrogas;
                    } else {
                        throw new BuildQueryException("A quantidade de campos e de valores não coincidem -> ".json_encode( $campos ),109);
                    }
                }
            }
            $this->campos_table = $campos;
        }

        return $this;

    }

    /**
     * Verify brackets in sql code
     * $insert = true, is to be added in the bank, then returns to ?
     * 
     * @param [type] $valor
     * @param boolean $insert
     * @return void
     */
    protected function verificarParentese($valor,$insert=false)
    {
        preg_match("/\((.*?)\)/", $valor, $in_parenthesis);
        $qntd_in_parenthesis = count($in_parenthesis);
        $params_subs = $qntd_in_parenthesis >= 1 ? explode(',',$in_parenthesis[1]) : [];
        $qntd_params = count($params_subs);

        if($insert)
            $valor = $qntd_params > 0 ? "(".str_pad('?',($qntd_params + ($qntd_params - 1)),',?',STR_PAD_RIGHT).")" : '?';
        else
            $valor = $qntd_params > 0 ? $params_subs : $valor;

        return $valor;
    }

    /**
     * Add array of value to insert DML
     *
     * @param [type] $array
     * @param boolean $delimitador
     * @return void
     */
    protected function addArrayValoresInsert($array, $delimitador=false)
    {
        $array = $delimitador != false ? explode($delimitador, $array) : $array;
        for($i = 0; $i < count($array); $i++) {
            $this->valores_insert[] = $array[$i];
        }
        return $this->valores_insert;
    }

    /**
     * Pre Adjust where and the normalize code sql with where
     *
     * @param [type] $tipo
     * @param [type] $campo
     * @param [type] $operador_comparacao
     * @param [type] $valor
     * @param boolean $status_where
     * @return void
     */
    protected function preAdjustWhere($tipo, $campo, $operador_comparacao, $valor, $status_where = false)
    {
        $valor_campo = "";
        if(strcmp($operador_comparacao, 'is') != 0) {
            $valor_add = $this->verificarParentese($valor);
            if (is_array($valor_add)) {
                $this->valores_insert = $this->addArrayValoresInsert($valor_add);
            } else {
                $this->valores_insert[] = $valor_add;
            }
            $valor_campo = $this->verificarParentese($valor, true);
        } else {
            $valor = is_null($valor) ? 'null' : $valor;
            $operador_comparacao .= ' '.$valor;
        }
        $this->ajustarWhere($tipo,$campo,$operador_comparacao,$valor_campo, $status_where);
    }

    /**
     * Set Where in DML
     *
     * @param [type] $campo
     * @param [type] $operador_comparacao
     * @param [type] $valor
     * @return void
     */
    public function where($campo,$operador_comparacao,$valor)
    {
        $this->preAdjustWhere("where",$campo,$operador_comparacao,$valor);
        return $this;
    }

    /**
     * Set where with OR comparation. Necessary Where before
     *
     * @param [type] $campo
     * @param [type] $operador_comparacao
     * @param [type] $valor
     * @return void
     */
    public function whereOr($campo,$operador_comparacao,$valor)
    {
        $this->preAdjustWhere("or",$campo,$operador_comparacao,$valor,$this->where);
        return $this;
    }

    /**
     * Set where with AND comparation. Necessary Where before
     *
     * @param [type] $campo
     * @param [type] $operador_comparacao
     * @param [type] $valor
     * @return void
     */
    public function whereAnd($campo,$operador_comparacao,$valor)
    {
        $this->preAdjustWhere("and",$campo,$operador_comparacao,$valor,$this->where);
        return $this;
    }

    /**
     * Whre with brackets and the various comparation. Necessary Where before
     *
     * @param [type] $campos
     * @param [type] $operadores
     * @param [type] $valores
     * @param boolean $oper_logicos
     * @return void
     */
    public function whereComplex($campos, $operadores, $valores, $oper_logicos=false)
    {
        if(!is_array($campos)) {
            $this->msg_erro = "No where complexo os campos precisam ser um array";
        }
        elseif(!is_array($operadores)) {
            $this->msg_erro = "No where complexo os operadores precisam ser um array";
        }
        elseif(!is_array($valores)) {
            $this->msg_erro = "No where complexo os valores precisam ser um array";
        }
        elseif(!is_array(@$oper_logicos)) {
            $this->msg_erro = "No where complexo os operadores lógicos precisam ser um array";
        } else {
            if($this->where == false) {
                $this->msg_erro = "Para utilizar o where complexo, é necessário instanciar o where primeiro";
            } else {
                $cont_campos = count($campos);
                $cont_operadores = count($operadores);
                $cont_valores = count($valores);
                $cont_logicos = count($oper_logicos);

                $tudo = array($cont_campos,$cont_operadores,$cont_valores,$cont_logicos);

                foreach ($tudo as $key => $comp) {
                    if($tudo[0] != $comp) {
                        $this->msg_erro = "As quantidade de valores não são equivalentes, por favor corrija";
                    }
                }

                if($this->msg_erro == false) {
                    $string = '';

                    for($i = 0; $i < count($campos); $i++) {
                        $interrogacoes = "";
                        if(strcmp($operadores[$i], 'is') != 0) {
                            $valor_add = $this->verificarParentese($valores[$i]);
                            $interrogacoes = $this->verificarParentese($valores[$i], true);
                            if (is_array($valor_add)) {
                                $this->valores_insert = $this->addArrayValoresInsert($valor_add);
                            } else {
                                $this->valores_insert[] = $valor_add;
                            }
                        } else {
                            $valor_opera = is_null($valores[$i]) ? 'null' : $valores[$i];
                            $operadores[$i] .= ' '.$valor_opera;
                        }

                        if($i == 0) {
                            $string .= $oper_logicos[0]." (".$campos[0]." ".$operadores[0]." ".$interrogacoes." ";
                        } elseif($i == count($campos) - 1) {
                            $string .=	$oper_logicos[$i]." ".$campos[$i]." ".$operadores[$i]." ".$interrogacoes." )";
                            $this->whereComplex[] = $string;
                        } else {
                            $string .= $oper_logicos[$i]." ".$campos[$i]." ".$operadores[$i]." ".$interrogacoes." ";
                        }
                    }
                }
            }
        }
        return $this;

    }

    /**
     * Set LEFT JOIN
     *
     * @param [type] $tabela_join
     * @param [type] $comparativo
     * @return void
     */
    public function leftJoin($tabela_join,$comparativo)
    {
        $this->ajustarJoin("leftJoin", $this->table_in,$tabela_join,$comparativo);
        return $this;
    }

    /**
     * Set RIGHT JOIN
     *
     * @param [type] $tabela_join
     * @param [type] $comparativo
     * @return void
     */
    public function rightJoin($tabela_join,$comparativo)
    {
        $this->ajustarJoin("rightJoin", $this->table_in,$tabela_join,$comparativo);
        return $this;
    }

    /**
     * Set INNER JOIN
     *
     * @param [type] $tabela_join
     * @param [type] $comparativo
     * @return void
     */
    public function innerJoin($tabela_join,$comparativo)
    {
        $this->ajustarJoin("innerJoin", $this->table_in,$tabela_join,$comparativo);
        return $this;
    }

    /**
     * Set Full Outer Join
     *
     * @param [type] $tabela_join
     * @param [type] $comparativo
     * @return void
     */
    public function fullOuterJoin($tabela_join,$comparativo)
    {
        $this->ajustarJoin("fullOuterJoin", $this->table_in,$tabela_join,$comparativo);
        return $this;
    }

    /**
     * Set GROUP BY
     *
     * @param [type] $tabela
     * @return void
     */
    public function groupBy($campos)
    {
        $this->ajustarGroupBy($campos);
        return $this;
    }

    /**
     * Set GroupBy Having
     *
     * @param [type] $tabela
     * @param [type] $clausula
     * @return void
     */
    public function groupByHaving($campos,$clausula)
    {
        $this->ajustarGroupBy($campos,$clausula);
        return $this;
    }

    /**
     * Set Order BY. Is possible using in whatever code flow with class instance
     *
     * @param [type] $campo
     * @param [type] $tipo
     * @return void
     */
    public function orderBy($campo, $tipo)
    {
        if(!is_string($campo)) {
            $this->msg_erro = "O campo precisa ser uma string";
        }
        elseif(!is_string($tipo) AND (strtoupper($tipo) == "ASC" OR strtoupper($tipo) == "DESC")) {
            $this->msg_erro = "O tipo precisa ser uma string, sendo ou ASC ou DESC ";
        } else {
            if($this->orderBy == false) {
                $this->orderBy = "ORDER BY " . $campo . " " . $tipo;
            } else {
                $this->orderBy = $this->orderBy.', '.$campo.' '.$tipo;
            }
        }

        return $this;
    }

    /**
     * Generate the insert with select in table and your fields
     *
     * @param [type] $tabela
     * @param [type] $campos
     * @return void
     */
    public function insertSelect($tabela,$campos)
    {
        if(!is_array($campos)) {
            throw new BuildQueryException("Os campos do insertSelect precisam ser passados em um array");
        } elseif(!is_string($tabela)) {
            throw new BuildQueryException("A tabela do insertSelect precisa ser uma String");
        } else {
            $campos_usar = implode(",",$campos);
            $this->insertSelect = "SELECT ".$campos_usar." FROM ".$tabela;
        }
        return $this;
    }

    /**
     * Flag to init union in tables
     *
     * @param boolean $tipo
     * @return void
     */
    public function union($tipo=false)
    {
        $this->limparValores(true);

        switch( strtolower($tipo) ) {
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
    
    /**
     * Set the msg to trigger in exception if nothing found
     *
     * @param [type] $msg
     * @return void
     */
    public function setMsgNaoEncontrado($msg)
    {
        $this->nao_encontrado_per = $msg;
        return $this;
    }

    /**
     * Set the limit to returns data
     *
     * @param [type] $limite
     * @param boolean $offset
     * @return void
     */
    public function limit($limite, $offset=false)
    {
        if($limite != false) {
            $this->limite = $limite;
            $this->offset = $offset;

            $this->valores_insert_final[] = (int) $limite;
            //$this->valores_insert[] = (int) $limite;
        }

        if($offset != false) {
            $this->valores_insert_final[] = (int) $offset;
        }

        return $this;
    }

    /**
     * Flag indicate what generate log of sql code
     *
     * @param boolean $gerar
     * @return void
     */
    public function setGerarLog($gerar=true)
    {
        $this->gerar_log = $gerar;
        return $this;
    }

    /**
     * Flag indicate if use or not execption in not found
     *
     * @param boolean $usar
     * @return void
     */
    public function setUsarExceptionNaoEncontrado($usar=true)
    {
        $this->exception_not_found = $usar;
        return $this;
    }

    /**
     * Set returns (Boolean) false in nothing found
     *
     * @param boolean $usar
     * @return void
     */
    public function setFalseNaoEncontrado($usar = true)
    {
        $this->retornar_false_not_found = $usar;
        return $this;
    }
    
    /**
     * Set return the affected lines in DML execution (INSERT, UPDATE, DELETE)
     *
     * @param [type] $linhas_afetadas
     * @return void
     */
    protected function setLinhasAfetadas($linhas_afetadas)
    {
        $this->linhas_afetadas = $linhas_afetadas;
    }

    /**
     * Get affected lines in DML execution
     *
     * @return void
     */
    public function getLinhasAfetadas()
    {
        return $this->linhas_afetadas;
    }

    /**
     * Set personalizated return in found data
     *
     * @param [type] $retorno
     * @return void
     */
    public function setRetornoPersonalizado($retorno)
    {
        if(!is_array($retorno)) {
            throw new BuildQueryException('Tipo de retorno personalizado não é um array',8);
        }
        $this->retorno_personalizado = $retorno;
        return $this;
    }

    /**
     * Set Events with disparate in array $eventos
     *
     * @param [type] $eventos
     * @return void
     */
    public function setEventosGravar($eventos)
    {
        if(is_array($eventos)) {
            $this->eventos_gravar = @array_map('strtoupper', $eventos);
        } else {
            throw new BuildQueryException('Os tipos de eventos passados não são um array', 15165);
        }
        return $this;
    }

    /**
     * Sinalize with log complex in use
     *
     * @return void
     */
    public function setLogandoComplexo()
    {
        $this->gravar_log_complexo = true;
        return $this;
    }

    /**
     * Set log complex 
     *
     * @param [type] $query
     * @param [type] $parametros
     * @return void
     */
    protected function setLogComplexo($query, $parametros)
    {
        if($this->eventos_gravar != false){
            if(in_array($this->method, $this->eventos_gravar)) {
                if(is_callable($this->eventos_retornar)) $this->eventos_retornar($this->method, $query, $parametros);
            }
        }
    }

    /**
     * Get Where generated
     *
     * @return void
     */
    public function getFullWhere()
    {
        $where = ($this->where != false) ? " ".$this->where : "";
        $whereComplex = ($this->whereComplex != false) ? " ".implode(" ",$this->whereComplex) : "";
        $whereAnd = $this->whereAnd != false ? " ".implode(" ",$this->whereAnd) : '';
        $whereOr = $this->whereOr != false ? " ".implode(" ",$this->whereOr) : '';

        return $where
            . $whereOr
            . $whereAnd
            . $whereComplex;
    }

    /**
     * Build the sql code DML
     *
     * @param [type] $tipo
     * @param boolean $usando_union
     * @return void
     */
    public function buildQuery($tipo, $usando_union=false)
    {
        $this->create($tipo,$this->table_in);

        if(isset($this->campos_table) AND !is_array($this->campos_table) AND $this->method != "DELETE") {
            $this->msg_erro = "Os campos não são um array";
            $code_error = 005;

        }
        elseif(!is_string($this->table)) {
            $this->msg_erro = "A tabela precisa ser uma string";
            $code_error = 006;
        }

        if(isset($this->msg_erro)) {
            if($this->msg_erro != false) {
                $msg = (isset($msg)) ? $msg : $this->msg_erro;
                $code_erro_return = isset($code_error) ? $code_error : 405;
                throw new BuildQueryException($msg, $code_erro_return);
            }

        }

        $campos_usar = (isset($this->campos_table) && $this->campos_table != false) ? implode(",",$this->campos_table) : "*";


        $orderby = ($this->orderBy != false) ? " ".$this->orderBy : "";
        $union = ($this->union != false && $this->unionAll == false) ? $this->union : '';
        $unionAll = ($this->union == false && $this->unionAll != false) ? $this->unionAll : '';
        $msg_nao_encontrado = ($this->nao_encontrado_per != false) ? $this->nao_encontrado_per : 'Nada Encontrado';

        // Joins
        $leftjoin = ($this->leftJoin != false) ? implode(' ',$this->leftJoin) : "";
        $rightjoin = ($this->rightJoin != false) ? implode(' ',$this->rightJoin) : "";
        $innerjoin = ($this->innerJoin != false) ? implode(' ',$this->innerJoin) : "";
        $fullouterjoin = ($this->fullOuterJoin != false) ? implode(' ',$this->fullOuterJoin) : "";
        // Fim Joins

        $groupby = ($this->groupBy != false) ? $this->groupBy." " : "";
        $limite = ((string) $this->limite != false) ? ' LIMIT ?' : "";
        $limite = ( (string) $this->offset != false) ? $limite.' OFFSET ?' : $limite;

        $retorno_personalizado = $this->retorno_personalizado;

        if($this->method == "SELECT") {
            $is_select = true;
            switch ($this->driver) {
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
                . $this->getFullWhere()
                . $groupby
                . $orderby
                . $limite;

        } elseif ($this->method == "INSERT") {
            if($this->list_inter == false AND $this->insertSelect == false) {
                throw new BuildQueryException("É ncessário passar os valores dos campos",007);

            } else {
                switch ($this->insertSelect) {
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
                            . $this->getFullWhere()
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
            if($campos_usar != '*') {
                //for ($i = 0; $i < count($this->campos_table); $i++) {
                foreach ($this->campos_table as $i => $campos_use) {
                    //$campos_use = $this->campos_table;
                    $campos_atualizar[] = $campos_use/*[$i]*/ . ' = ? ';
                }
                //}

                $campos_usar = implode(',',$campos_atualizar);

                $string_build = $this->method . " "
                    . $this->table . " "
                    . $this->util . " "
                    . $campos_usar . ""
                    . $this->getFullWhere();
            } else {
                throw new BuildQueryException('Layout incorreto para o método UPDATE ',107);
            }
        } elseif($this->method == "DELETE") {
            $string_build = $this->method . " "
                . $this->util . " "
                . $this->table . ""
                . $this->getFullWhere()
                . $groupby;
        } else {
            throw new BuildQueryException("Metódo desconhecido", 108);

        }

        $this->query_union .= $union
            .$unionAll
            .$string_build;
        $this->valores_insert_bd[] = $this->valores_insert;
        $pdo_obj = $this->pdo_obj_usando != false ? $this->pdo_obj_usando : false;

        if($this->gravar_log_complexo == false) {
            $this->setLogComplexo($this->gravarsetLogComplexo, $this);
            $this->gravar_log_complexo = true;
        }

        $query = $this->query_union;
        $parametros = false;

        $count_insert_bd = count($this->valores_insert_bd);

        if($count_insert_bd > 0) {
            $dados_insert_query = [];

            foreach ($this->valores_insert_bd as $i => $matriz_insert) {
                foreach ($matriz_insert as $j => $elemento_insert) {
                    $dados_insert_query[] = $elemento_insert;
                }
            }

            $dados_insert_query = is_array($dados_insert_query) ? $dados_insert_query : [$dados_insert_query];

            if (count($this->valores_insert_final) > 0) {
                $dados_insert_query = array_merge($dados_insert_query, $this->valores_insert_final);
            }

            $parametros = $dados_insert_query;
        }

        $executar = $this;


        $this->query_mounted = [$query, $parametros];
        
        if(!$usando_union) $executar = $this->execSql($query, $parametros);

        if(!$usando_union) {
            $this->limparValores();
            $this->query_union = '';
        }

        if($retorno_personalizado != false) return array_merge(["DADOS"=>$executar], $retorno_personalizado);

        return $executar;
    }

    /**
     * Return the query mounted for build query
     *
     * @return void
     */
    public function getQueryMounted()
    {
        return $this->query_mounted;  
    }

    /**
     * Metho to show tables in database
     *
     * @return void
     */
    public function showTables() 
    {
        $retorno = false;

        switch($this->driver) {
            case 'mysql':
                $retorno = $this->execSql('SHOW TABLES');
                break;
            case 'sqlite':
                $retorno = $this->tabela('sqlite_master')->campos(['*'])->where('type','=','table')->buildQuery('select');
                break;
            case 'postgres':
                $retorno = $this->tabela('pg_catalog.pg_tables')->campos(['*'])->buildQuery('select');
                break;
            case 'firebird':
                break;
        }

        return $retorno;
    }

    /**
     * Method to set fields DDL to inser table
     * 
     * Waiting for
     * [
     *  'field_name' => ['type' => 'integer', 'options_field' => ['NOT NULL','TESTE', 'PRIMARY KEY AUTOINCREMENT'] ]
     * ]
     *
     * @param Array $campos
     * @return void
     */
    public function camposDdlCreate(Array $campos, $primary_key = false) 
    {

        if(count($campos) == 0) throw new BuildQueryException('O array de campos DDL não pode ser vazio', 7845);

        $this->campos_ddl = [];

        $marcador = $this->getMarcador();

        foreach($campos as $campo_nome => $campo_opcoes) {
            
            if(!is_string($campo_nome)) throw new BuildQueryException('O nome do campo precisa ser uma String', 7846);

            if(!isset($campo_opcoes['type']) OR @empty($campo_opcoes['type'])) throw new BuildQueryException('É necessário passar o tipo do campo', 7847);
            if(!isset($campo_opcoes['options_field']) OR @empty($campo_opcoes['options_field'])) throw new BuildQueryException('É necessário passar as opções do campo', 7848);
            
            if(strtolower($primary_key) == strtolower($campo_nome)) {
                if($this->driver == 'sqlite')  {
                    array_splice($campo_opcoes['options_field'], 2, 0, 'PRIMARY KEY');
                }
            }

            $campo_opcoes['options_field'] = array_map('strtoupper', $campo_opcoes['options_field']);
               
            $this->campos_ddl[] = "$marcador".$campo_nome."$marcador ".strtoupper($campo_opcoes['type'])." ".implode(' ', $campo_opcoes['options_field']);
        }

        if($primary_key != false and $this->driver == 'mysql') {
            $this->campos_ddl[] = "PRIMARY KEY ($marcador".$primary_key."$marcador)";
        }
        
        return $this;
    }

    /**
     * Set the MySql engine table
     *
     * @param [type] $engine
     * @return void
     */
    public function setEngineMysql($engine) 
    {
        $this->engineMysql = 'ENGINE = ' . $engine;

        return $this;
    }

    /**
     * Set default character for table
     *
     * @param [type] $character
     * @return void
     */
    public function setDefaultCharacter($character) 
    {
        $this->characterMysql = 'DEFAULT CHARACTER SET = ' . $character;

        return $this;
    }

    /**
     * Set collate for table
     *
     * @param [type] $collate
     * @return void
     */
    public function setCollate($collate) 
    {
        $this->collateMysql = 'COLLATE = ' . $collate;

        return $this;
    }

    /**
     * Set Foreign Key for table
     *
     * @param [type] $foreign_key_name
     * @param [type] $refence
     * @param string $onDelete
     * @param string $onUpdate
     * @return void
     */
    public function setForeignKey($foreign_key_name, Array $refence, $onDelete = 'NO ACTION', $onUpdate = 'NO ACTION') 
    {
        $marcador = $this->getMarcador();

        if(count((array) $this->campos_table) == 0) throw new BuildQueryException('É necessário informar os campos que comporão a foreign key', 488213);
        if(count($refence) != 2) throw new BuildQueryException('A referência da chave estrangeira não é um array de duas posições', 41231);
        if(@empty($refence['tabela']) OR @empty($refence['campos'])) throw new BuildQueryException('Não foi informado o campo '.(@empty($refence['tabela']) ? 'tabela' : 'campos').' da foreign key', 41231);

        $campos_foreign = implode($marcador.' ', $this->campos_table);

        $refence['campos'] = is_array($refence['campos']) ? $refence['campos'] : [$refence['campos']];
        $refence['campos'] = implode($marcador.' ', $refence['campos']);

        $validarAction = function($event, $event_name) {
            $actions_permitidas_por_driver = [
                'mysql' => ['RESTRICT','CASCADE','SET NULL','NO ACTION'],
                'postgres' => ['RESTRICT','CASCADE','SET NULL','NO ACTION'],
                'sqlite' => ['RESTRICT','CASCADE','SET NULL','SET DEFAULT','NO ACTION'],
                'firebird' => ['RESTRICT','CASCADE','SET NULL','NO ACTION']
            ];

            if($event != false) {
                if(!in_array($event, $actions_permitidas_por_driver[$this->driver])) throw new BuildQueryException('Action não permitida em '.$event_name, 41556);
            }

            return strtoupper($event);
        };

        $onDelete = $validarAction($onDelete,'onDelete');
        $onUpdate = $validarAction($onUpdate,'onUpdate');

        $foreign_key = "CONSTRAINT $marcador".$foreign_key_name."$marcador".PHP_EOL;
        $foreign_key .= "FOREIGN KEY (".$campos_foreign.") ".PHP_EOL;
        $foreign_key .= "REFERENCES $marcador".$refence['tabela']."$marcador (".$refence['campos'].") ".PHP_EOL;
        $foreign_key .= "ON DELETE " . $onDelete.PHP_EOL;
        $foreign_key .= "ON UPDATE " . $onUpdate.PHP_EOL;

        $this->foreign_key[] = $foreign_key;
        
        return $this;
    }

    /**
     * To create a table
     *
     * @return void
     */
    public function createTable() 
    {
        if(strlen((string) $this->table_in) == 0) throw new BuildQueryException('É preciso informar o nome da tabela', 8457);
        if(count((array) $this->campos_ddl) == 0) throw new BuildQueryException('É preciso informar os campos que serão adicionados', 8458);

        $marcador = $this->getMarcador();
        
        $string = "CREATE TABLE $marcador". $this->table_in ."$marcador (".PHP_EOL;
        
        if(count((array) $this->foreign_key) > 0) {
            $this->campos_ddl[] = implode(', ', $this->foreign_key);
        }

        $string .= implode(', '.PHP_EOL, $this->campos_ddl);

        $string .= ')';

        if($this->driver == 'mysql') {
            if(strlen((string) $this->engineMysql) > 0) $string .= PHP_EOL.$this->engineMysql;
            if(strlen((string) $this->characterMysql) > 0) $string .= PHP_EOL.$this->characterMysql;
            if(strlen((string) $this->collateMysql) > 0) $string .= PHP_EOL.$this->collateMysql;
        }

        $string .= ';';

        $this->iniciarTransacao();

        $retornar = $this->execSql($string);

        $this->commit();

        return $retornar;
    }

    /**
     * To Drop a table
     *
     * @return void
     */
    public function dropTable()
    {
        if(strlen((string) $this->table_in) == 0) throw new BuildQueryException('É preciso informar o nome da tabela', 8457);

        $marcador = $this->getMarcador();

        $this->iniciarTransacao();
        
        $retornar = $this->execSql("DROP TABLE IF EXISTS $marcador".$this->table_in."$marcador  ");

        $this->commit();

        return $retornar;
    }

    /**
     * To create a view
     *
     * @param [type] $nome_view
     * @return void
     */
    public function createView($nome_view) 
    {
        $marcador = $this->getMarcador();

        $querySelect = $this->getQueryMounted();

        if(is_null($querySelect)) throw new BuildQueryException('É necessário montar um select para criar uma view', 9842);

        $query = $this->getQueryMounted()[0];
        $parametros = $this->getQueryMounted()[1];

        $string = "CREATE VIEW $marcador".$nome_view."$marcador AS ".PHP_EOL;
        $string .= $query.';;';

        $this->iniciarTransacao();
        
        $retornar = $this->execSql($string, $parametros);

        $this->commit();

        return $retornar;
    }

    /**
     * To drop a view
     *
     * @param [type] $nome_view
     * @return void
     */
    public function dropView($nome_view)
    {
        //DROP VIEW `test`.`new_view`;
        $marcador = $this->getMarcador();
        
        $string = "DROP VIEW $marcador".$nome_view."$marcador";

        $this->iniciarTransacao();
        
        $retornar = $this->execSql($string);

        $this->commit();

        return $retornar;
    }
    
    /**
     * Get information bd using pdo attributes
     * https://www.php.net/manual/pt_BR/pdo.getattribute.php
     *
     * @param array $attribute
     * @return void
     */
    public function getInformationDb($attribute = [])
    {
        $default = ["DRIVER_NAME","CLIENT_VERSION", "SERVER_VERSION"];
        
        $attribute = is_array($attribute) ? $attribute : [$attribute];

        if(is_array($attribute)) array_merge($default, $attribute);
        $array_retorno_info = [];

        foreach ($default as $val) {
            if(!is_string($val)) throw new BuildQueryException('O attributo '.print_r($val, true).' não é válido, pois é um array', 104578);
            
            $val = strtoupper($val);

            try {
                $array_retorno_info[$val] = $this->pdo()->getAttribute(constant("PDO::ATTR_$val"));
            } catch (PDOException $e) {
                throw new BuildQueryException('Erro Attribute: ' . $val . ' -> ' . $e->getMessage(), $e->getCode());
            }
        }

        return $array_retorno_info;
    }

    public function setContentTrigger($content)
    {

    }

    public function createTigger($execution)
    {
        /** 
         *     CREATE DEFINER = CURRENT_USER TRIGGER `test`.`new_table_BEFORE_INSERT` BEFORE INSERT ON `new_table` FOR EACH ROW
*BEGIN

*END
         */
    }
}