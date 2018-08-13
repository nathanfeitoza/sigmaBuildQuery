[![Build Status](https://travis-ci.org/nathanfeitoza/sigmaBuildQuery.svg?branch=master)](https://travis-ci.org/nathanfeitoza/sigmaBuildQuery)

**SigmaBuildQuery** `beta`

A build query php to make SQL executions easier by standardizing them. Databases available: Mysql, Postgres, Firebird, Sqlite

To start BuildQuery, just make the following call:
   ```php
    $var = \Sigma\BuildQuery::init( (string) 'driver',(string) 'host',(string) 'database',(string) 'user',(string) 'pass'[, (array) options);
   ```
After doing this, we have the method of executing SQL scripts (handwritten SQL)
   ```php
    $var->ExecSql( (string) query, (array) campos [, (boolean) use_transaction, (boolean) use_exception_not_found] );
   ```
   If use_transaction is set to true, it will begin to use database transactions (which have this option: Firebird tested so far)
The methods of querybuilder are shown below:
        Note: The use of the entire query builder is done via polymorphism, which are being shown below. The choice by this method has been established because it looks more like sql queries and block building of codes. Therefore, it facilitates the life of the developer, being that the order of the elements will not change the final result, unless a main element such as -> table (string) is missing, but this check is already done and triggered in the log (to be implemented)

   ```php
            $var->FazerRollback() // Rollback if there is any open transaction. Can be used when mixing code with transaction with no transaction. Obs: Does not polymorphism because it is a method of containment / prevention of errors
            
            $var->tabela('teste') // Sets the usage table
            ->campos(array("terste1","teste2","teste3")) // Fields used to make select, one can only pass an empty array: [''], and it will search all the fields of the table, or ['*'], or the field names
            ->campos(array("terste1","teste2","teste3"),array("valor1","valor2","valor3")) // Fields and their respective values to be inserted or updated
            ->insertSelect("testar",array("campo1","campo2")) // To make an insert using a select, insert test set (select field1, field2 from table1)
            ->leftjoin("tabela b","a.id = b.id") // To use left join
            ->rightjoin("tabela b","a.id = b.id") // To use right join
            ->innerjoin("tabela b","a.id = b.id") // To use inner join
            ->fullouterjoin("tabela b","a.id = b.id") // To use full outer join
            ->where("teste","=",123) // For where use, where the first method is the fields, the second the comparative and the third the value to be compared
            ->whereComplex(array("testaco","testinho","testar","testei"),array("=","!=","=","!="),array("456","789","856","1"),array("OR", "AND","OR","OR")) // For a where with multiple attributes. Ex: WHERE (field = 1) AND (field2 = 3) OR (fields3 = 2)
            ->whereComplex(array("testaco","testinho","testar"),array("=","!=","="),array("456","789","856"),array("OR", "AND","OR"))
            ->whereOr("testar","!=",456) // The same as Where, but put the OR in front, this way, where it should be called before, otherwise it will cause an sql error
            ->whereAnd("testando","=",321) // Same as whereOr, however add the And
            ->groupby("tabelinha1") // To use groupby
            ->groupbyHaving("tabelinha1","teste = teste") // To use GROUP BY HAVING
            ->orderby("id","ASC") // For sorting, where the first method is the field and the second sorting type
            ->GerarLog(true) // To generate logs with the execution query in the database -> true or false (Making)
            ->limit((int) 100 [,(int) offset]) // To add a limit and also offset (offset only in postgres) to the search (functional only in mysql and postgres)
            ->UsarExceptionNaoEncontrado(true) // To trigger an exception if no result is found in a select, if true. If false, it will fire an array of two elements, the first containing a string saying nothing was found, and the second with error code (710). By default it is true
            ->buildQuery("select", true) // This method executes the query, being defined as: buildQuery ((string) exec_type, (boolean) usar_union, (boolean) usar_transaction). The first one refers to the type of call that will be made: select, update, delete, insert
            ->union('all') // To make the union between two tables. It allows its use by setting 'all', 'union' or empty. To work, it is necessary that the previous buildQuery is set to use_union
            ->tabela("teste3")
            ->campos(array("testar"), array("testarV"))
            ->buildQuery("select", true)
            ->union()
            ->ComTransaction((contador atual) // This method activates the transaction and for this it is necessary to define the initial position of the counter and the final position. If it is not in a loop, you can put the values 1 and 1. But if it is in an array loop, for example, place ($ i, (count ($ array) - 1))
            ->tabela("teste4")
            ->campos(array("testar","testarheuhe"), array("testarV","testeF"))
            ->buildQuery("select");
   ```

   Example using simple transaction
   ```php
        $total = 3;
        $trans = $this->getConBD()->InicarTransacao(); // Is necessary for maintaining the PDO Object
        for($i = 0; $i < $total; $i++) {
            $dados_add = $i;
            $data = $trans
                ->tabela('teste')
                ->campos(['log','testei'], ['teste-'.$i,$dados_add])
                ->GerarLog(true)
                ->comTransaction($i, ($total - 1))
                ->buildQuery('insert');
        }
   ```

   Example using multiples tables with transactions
   ```php
        for($i = 0; $i < 100; $i++) {
            $data = $this->getConBD()
                ->tabela('teste')
                ->campos(['log','testei'], ['teste-'.$i, 1])
                ->GerarLog(true)
                ->TransacaoMultipla()
                ->buildQuery('insert', true)
                ->tabela('teste2')
                ->campos(['nome','teste'], ['teste_tabela2-'.$i, 1])
                ->GerarLog(true)
                ->CompletarTransacaoMultipla()
                ->buildQuery('insert');
        }
   ```
   Varying number of values entered in table 2
   ```php
        $percorrer = 100;
        $data = $this->getConBD()
            ->tabela('teste')
            ->campos(['log','testei'], ['teste-0', 1])
            ->GerarLog(true)
            ->TransacaoMultipla()
            ->buildQuery('insert', true);
        for($i = 0; $i < $percorrer; $i++) {
                $add = 1;
                $data->tabela('teste2')
                ->campos(['nome','teste'], ['teste_tabela2-'.$i, $add])
                ->GerarLog(true);
                
                if(($i+1) < $percorrer) {
                    $data->TransacaoMultipla()
                        ->buildQuery('insert', true);
                } else {
                    $data->CompletarTransacaoMultipla()
                        ->buildQuery('insert');
                }
        }
   ```
