#include <eosio/sql_db_plugin/traces_table.hpp>

#include <chrono>
#include <fc/log/logger.hpp>

namespace eosio {

    traces_table::traces_table(std::shared_ptr<soci::session> session):
        m_session(session) {

    }

    void traces_table::drop() {
        try {
            *m_session << "DROP TABLE IF EXISTS traces";
            *m_session << "DROP TABLE IF EXISTS assets";
            *m_session << "DROP TABLE IF EXISTS tokens";
        }
        catch(std::exception& e){
            wlog(e.what());
        }
    }

    void traces_table::create() {
        *m_session << "CREATE TABLE `traces` ("
                "`tx_id` bigint(20) NOT NULL AUTO_INCREMENT, "
                "`id` varchar(64) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',"
                "`data` json DEFAULT NULL,"
                "`irreversible` tinyint(1) NOT NULL DEFAULT '0',"
                "PRIMARY KEY (`tx_id`),"
                "UNIQUE INDEX `idx_transactions_id` (`id`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;";

        *m_session << "CREATE TABLE `assets`  ("
                "`symbol_owner` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,"
                "`amount` double(64, 30) NULL DEFAULT NULL,"
                "`max_amount` double(64, 30) NULL DEFAULT NULL,"
                "`symbol_precision` int(2) NULL DEFAULT NULL,"
                "`symbol` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                "`issuer` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                "`owner` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                "PRIMARY KEY (`symbol_owner`) USING BTREE"
                ") ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;";

        *m_session << "CREATE TABLE `tokens`  ("
                "`id` bigint(20) NOT NULL AUTO_INCREMENT,"
                "`account` varchar(16) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',"
                "`symbol` varchar(16) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',"
                "`amount` double(64, 4) NOT NULL DEFAULT 0.0000,"
                "`symbol_owner` varchar(30) COLLATE utf8mb4_general_ci NULL DEFAULT NULL,"
                "`symbol_owner_account` varchar(50) COLLATE utf8mb4_general_ci NOT NULL,"
                "PRIMARY KEY (`id`) USING BTREE,"
                "INDEX `idx_tokens_account`(`account`) USING BTREE,"
                "UNIQUE INDEX `idx_tokens_symbolowneraccount`(`symbol_owner_account`) USING BTREE"
                ") ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci;";

    }

    void traces_table::add( const chain::transaction_trace_ptr& trace) {
        const auto trace_id_str = trace->id.str();
        const auto data = fc::json::to_string(trace);
        try{
            *m_session << "REPLACE INTO traces(id, data) "
                        "VALUES (:id, :data)",
                soci::use(trace_id_str),
                soci::use(data);
                
        } catch (std::exception e) {
            wlog( "${e} ${id} ${data}",("e",e.what())("id",trace_id_str)("data",data) );
        }catch(...){
            wlog("insert trace failed. ${id}",("id",trace_id_str));
        }
    }

    bool traces_table::list( std::string trace_id_str, chain::block_timestamp_type block_time){
        std::string data;
        block_timestamp = std::chrono::seconds{block_time.operator fc::time_point().sec_since_epoch()}.count();
        try{
            *m_session << "SELECT data FROM traces WHERE id = :id",soci::into(data),soci::use(trace_id_str);
        } catch(std::exception e) {
            wlog( "data:${data}",("data",data) );
            wlog("${e}",("e",e.what()));
        } catch(...){
            wlog( "data:${data}",("data",data) );
        }

        if(data.empty()){
            wlog( "trace data is null. ${id}",("id",trace_id_str) );
            return false;
        }
        auto trace = fc::json::from_string(data).as<chain::transaction_trace>();
        // ilog("${result}",("result",trace));
        dfs_inline_traces( trace.action_traces );

        try{
            *m_session << "DELETE FROM traces WHERE id = :id",soci::use(trace_id_str);
        } catch(std::exception e) {
            wlog( "data:${data}",("data",data) );
            wlog("${e}",("e",e.what()));
        } catch(...){
            wlog( "data:${data}",("data",data) );
        }

        return true;
    }

    void traces_table::dfs_inline_traces( vector<chain::action_trace> trace ){
        for(auto& atc : trace){
            if( atc.receipt.receiver == atc.act.account ){
                parse_actions(atc.act);
                if(atc.inline_traces.size()!=0){
                    dfs_inline_traces( atc.inline_traces );
                }
            }
        }
    }

    void traces_table::parse_actions( chain::action action ) {
        
        chain::abi_def abi;
        std::string abi_def_account;
        chain::abi_serializer abis;
        soci::indicator ind;

        *m_session << "SELECT abi FROM accounts WHERE name = :name", soci::into(abi_def_account, ind), soci::use(action.account.to_string());

        if (!abi_def_account.empty()) {
            abi = fc::json::from_string(abi_def_account).as<chain::abi_def>();
        } else if (action.account == chain::config::system_account_name) {
            abi = chain::eosio_contract_abi(abi);
        } else {
            return; // no ABI no party. Should we still store it?
        }

        abis.set_abi(abi, max_serialization_time);

        auto abi_data = abis.binary_to_variant(abis.get_action_type(action.name), action.data, max_serialization_time);

        if( action.account == chain::config::system_account_name ){

            if ( action.name == N(voteproducer) ){

                auto voter = abi_data["voter"].as<chain::name>().to_string();
                auto proxy = abi_data["proxy"].as<chain::name>().to_string();
                auto producers = fc::json::to_string( abi_data["producers"] );

                try{
                    *m_session << "INSERT INTO votes ( voter, proxy, producers )  VALUES( :vo, :pro, :pd ) "
                            "on  DUPLICATE key UPDATE proxy = :pro, producers =  :pd ",
                            soci::use(voter),
                            soci::use(proxy),
                            soci::use(producers),
                            soci::use(proxy),
                            soci::use(producers);
                } catch(std::exception e) {
                    wlog(" ${voter} ${proxy} ${producers}",("voter",voter)("proxy",proxy)("producers",producers));
                    wlog( "${e}",("e",e.what()) );
                } catch(...) {
                    wlog(" ${voter} ${proxy} ${producers}",("voter",voter)("proxy",proxy)("producers",producers));
                }


            } else if ( action.name == N(delegatebw) ){

                auto from = abi_data["from"].as<chain::name>().to_string();
                auto receiver = abi_data["receiver"].as<chain::name>().to_string();
                auto stake_net_quantity = abi_data["stake_net_quantity"].as<chain::asset>();
                auto stake_cpu_quantity = abi_data["stake_cpu_quantity"].as<chain::asset>();       
                auto transfer = abi_data["transfer"].as<bool>();

                if(transfer) from = receiver;

                try{
                    
                    if( from == receiver ){
                        // ilog("${transfer}",("transfer",transfer));
                        if(!transfer){
                            *m_session << "UPDATE refunds SET net_amount = ( CASE WHEN net_amount < :na THEN 0 ELSE net_amount - :na END ), "
                                "cpu_amount = ( CASE WHEN cpu_amount < :ca THEN 0 ELSE cpu_amount - :ca END) WHERE owner = :ow ",
                                soci::use(stake_net_quantity.to_real()),
                                soci::use(stake_net_quantity.to_real()),
                                soci::use(stake_cpu_quantity.to_real()),
                                soci::use(stake_cpu_quantity.to_real()),
                                soci::use(receiver);
                        }

                        *m_session << "INSERT INTO stakes ( account, net_amount_for_self, cpu_amount_for_self, net_amount_for_other,cpu_amount_for_other )  VALUES( :ac, :nam, :cam, 0, 0 ) "
                            "on  DUPLICATE key UPDATE net_amount_for_self = net_amount_for_self +  :nam, cpu_amount_for_self = cpu_amount_for_self + :cam ",
                            soci::use(receiver),
                            soci::use(stake_net_quantity.to_real()),
                            soci::use(stake_cpu_quantity.to_real()),
                            soci::use(stake_net_quantity.to_real()),
                            soci::use(stake_cpu_quantity.to_real());
                    }else{
                        *m_session << "INSERT INTO stakes ( account, net_amount_for_self, cpu_amount_for_self, net_amount_for_other, cpu_amount_for_other )  VALUES( :ac, 0, 0, :nam, :cam ) "
                            "on  DUPLICATE key UPDATE net_amount_for_other = net_amount_for_other +  :nam, cpu_amount_for_other = cpu_amount_for_other + :cam ",
                            soci::use(receiver),
                            soci::use(stake_net_quantity.to_real()),
                            soci::use(stake_cpu_quantity.to_real()),
                            soci::use(stake_net_quantity.to_real()),
                            soci::use(stake_cpu_quantity.to_real());
                    }

                } catch(std::exception e) {
                    wlog("${e}",("e",e.what()));
                } catch(...) {
                    wlog( "delegatebw ${from} delegate ${receiver} ${stake_net_quantity} ${stake_cpu_quantity} ",("from",from)("receiver",receiver)("stake_net_quantity",stake_net_quantity)("stake_cpu_quantity",stake_cpu_quantity) );
                }

            } else if ( action.name == N(undelegatebw) ) {

                auto from = abi_data["from"].as<chain::name>().to_string();
                auto receiver = abi_data["receiver"].as<chain::name>().to_string();
                auto unstake_net_quantity = -abi_data["unstake_net_quantity"].as<chain::asset>();
                auto unstake_cpu_quantity = -abi_data["unstake_cpu_quantity"].as<chain::asset>();

                try{

                    if(from == receiver){
                        *m_session << "INSERT INTO stakes ( account, net_amount_for_self, cpu_amount_for_self, net_amount_for_other, cpu_amount_for_other )  VALUES( :ac, :nam, :cam, 0, 0 ) "
                            "on  DUPLICATE key UPDATE net_amount_for_self = net_amount_for_self +  :nam, cpu_amount_for_self = cpu_amount_for_self + :cam ",
                            soci::use(receiver),
                            soci::use(unstake_net_quantity.to_real()),
                            soci::use(unstake_cpu_quantity.to_real()),
                            soci::use(unstake_net_quantity.to_real()),
                            soci::use(unstake_cpu_quantity.to_real());
                    }else{
                        *m_session << "INSERT INTO stakes ( account, net_amount_for_self, cpu_amount_for_self, net_amount_for_other, cpu_amount_for_other )  VALUES( :ac, 0, 0, :nam, :cam ) "
                            "on  DUPLICATE key UPDATE net_amount_for_other = net_amount_for_other +  :nam, cpu_amount_for_other = cpu_amount_for_other + :cam ",
                            soci::use(receiver),
                            soci::use(unstake_net_quantity.to_real()),
                            soci::use(unstake_cpu_quantity.to_real()),
                            soci::use(unstake_net_quantity.to_real()),
                            soci::use(unstake_cpu_quantity.to_real());
                    }
                    // ilog( "blocktime::" );
                    // ilog( "${bt}",("bt",block_timestamp) );
                    *m_session << "INSERT INTO refunds ( owner, request_time, net_amount, cpu_amount )  VALUES( :ac, FROM_UNIXTIME(:rt), :nam, :cam ) "
                            "on  DUPLICATE key UPDATE request_time = FROM_UNIXTIME(:rt), net_amount = net_amount +  :nam, cpu_amount = cpu_amount + :cam ",
                            soci::use(from),
                            soci::use(block_timestamp),
                            soci::use((-unstake_net_quantity).to_real()),
                            soci::use((-unstake_cpu_quantity).to_real()),
                            soci::use(block_timestamp),
                            soci::use((-unstake_net_quantity).to_real()),
                            soci::use((-unstake_cpu_quantity).to_real());

                } catch(std::exception e) {
                    wlog("${e}",("e",e.what()));
                } catch(...) {
                    wlog( "undelegatebw ${from} undelegate ${receiver} ${unstake_net_quantity} ${unstake_cpu_quantity} ",("from",from)("receiver",receiver)("unstake_net_quantity",unstake_net_quantity)("unstake_cpu_quantity",unstake_cpu_quantity) );
                }

            } else if ( action.name == N(refund) ){
                auto owner = abi_data["owner"].as<chain::name>().to_string();

                try{
                    *m_session << " UPDATE refunds SET net_amount = 0, cpu_amount = 0 WHERE owner = :ow",soci::use(owner);
                } catch(std::exception e) {
                    wlog("${e}",("e",e.what()));
                } catch(...){
                    wlog("refund ${owner}",("owner",owner));
                }

            }

        } else if( action.account == N(eosio.token) ) {

            if( action.name == N(create) ){

                auto issuer = abi_data["issuer"].as<chain::name>().to_string();
                auto maximum_supply = abi_data["maximum_supply"].as<chain::asset>();
                auto symbol_owner = (action.account.to_string() + "_" + maximum_supply.symbol_name());
                string insertassets;
                try{
                    insertassets = "INSERT assets(symbol_owner, amount, max_amount, symbol_precision, symbol, issuer, owner) VALUES( :so, :am, :mam, :pre, :sym, :issuer, :owner)";
                    *m_session << insertassets,
                            soci::use( symbol_owner ),
                            soci::use( 0 ),
                            soci::use( maximum_supply.to_real() ),
                            soci::use( maximum_supply.precision() ),
                            soci::use( maximum_supply.get_symbol().name() ),
                            soci::use( issuer ),
                            soci::use( action.account.to_string() );
                } catch(std::exception e) {
                    wlog("${e}",("e",e.what()));
                } catch (...) {
                    wlog("${sql}",("sql",insertassets) );
                    wlog( "create asset failed. ${issuer} ${maximum_supply}",("issuer",issuer)("maximum_supply",maximum_supply) );
                }

            } else if( action.name == N(issue) ){

                auto to = abi_data["to"].as<chain::name>().to_string();
                auto quantity = abi_data["quantity"].as<chain::asset>();
                auto symbol_owner = action.account.to_string() + "_" + quantity.get_symbol().name();
                string symbol_owner_account ;
                string issuer;

                try{
                    //update asset issue amount
                    *m_session << "UPDATE assets SET amount = amount + :am WHERE symbol_owner = :so",
                        soci::use( quantity.to_real() ),
                        soci::use( symbol_owner );

                    *m_session << "SELECT issuer FROM assets WHERE symbol_owner = :so",
                        soci::into( issuer ),
                        soci::use( symbol_owner );

                    symbol_owner_account = action.account.to_string() + "_" + issuer + "_" + quantity.get_symbol().name();

                    //add issue's assets and then will have a transfer action to transfer issue's amount to "to".
                    *m_session << "INSERT INTO tokens ( account, symbol, amount, symbol_owner, symbol_owner_account )  VALUES( :ac, :sym, :am, :so, :soac ) "
                            "on  DUPLICATE key UPDATE amount = amount +  :amt ",
                            soci::use( issuer ),
                            soci::use( quantity.get_symbol().name() ),
                            soci::use( quantity.to_real() ),
                            soci::use( symbol_owner ),
                            soci::use( symbol_owner_account ),
                            soci::use( quantity.to_real() );
                } catch(std::exception e) {
                    wlog("my god : ${e}",("e",e.what()));
                } catch(...) {
                    wlog( "INSERT INTO tokens ( account, symbol, amount, symbol_owner, symbol_owner_account )  VALUES( :ac, :sym, :am, :so, :soac ) " );
                    wlog( "issue asset failed. ${issuer} ${quantity}",("issuer",issuer)("quantity",quantity) );
                }

            } else if ( action.name == N(transfer) ){

                auto from = abi_data["from"].as<chain::name>().to_string();
                auto to = abi_data["to"].as<chain::name>().to_string();
                auto quantity = abi_data["quantity"].as<chain::asset>();
                auto symbol_owner = action.account.to_string() + "_" + quantity.get_symbol().name();
                auto symbol_owner_account = action.account.to_string() + "_" + to + "_" + quantity.get_symbol().name();
                auto symbol_owner_from = action.account.to_string() + "_" + from + "_" + quantity.get_symbol().name();

                try{
                    *m_session << "INSERT INTO tokens ( account, symbol, amount, symbol_owner, symbol_owner_account )  VALUES( :ac, :sym, :am, :so, :soac ) "
                            "on  DUPLICATE key UPDATE amount = amount +  :amt ",
                            soci::use( to ),
                            soci::use( quantity.get_symbol().name() ),
                            soci::use( quantity.to_real() ),
                            soci::use( symbol_owner ),
                            soci::use( symbol_owner_account ),
                            soci::use( quantity.to_real() );

                    *m_session << "UPDATE tokens SET amount = amount - :am WHERE symbol_owner_account = :soac ",
                            soci::use( quantity.to_real() ),
                            soci::use( symbol_owner_from );

                } catch(std::exception e) {
                    wlog("my god : ${e}",("e",e.what()));
                } catch(...) {
                    wlog( "transfer failed. ${from} transfer to ${to} ${quantity} ",("from",from)("to",to)("quantity",quantity) );
                }

            }

        } else {

          if( action.name == N(create) ){

                string issuer;
                chain::asset maximum_supply;

                try{ 
                    issuer = abi_data["issuer"].as<chain::name>().to_string();
                    maximum_supply = abi_data["maximum_supply"].as<chain::asset>();
                } catch(std::exception e) {
                    wlog( "create args transform variant failed ${account} ${e}",("account",action.account)("e",e.what()) );
                    return ;
                } catch(...) {
                    wlog( "create args transform vartient failed ${account}",("account",action.account) );
                    return ;
                }

                auto symbol_owner = (action.account.to_string() + "_" + maximum_supply.symbol_name());
                string insertassets;
                try{
                    insertassets = "INSERT assets(symbol_owner, amount, max_amount, symbol_precision, symbol, issuer, owner) VALUES( :so, :am, :mam, :pre, :sym, :issuer, :owner)";
                    *m_session << insertassets,
                            soci::use( symbol_owner ),
                            soci::use( 0 ),
                            soci::use( maximum_supply.to_real() ),
                            soci::use( maximum_supply.precision() ),
                            soci::use( maximum_supply.get_symbol().name() ),
                            soci::use( issuer ),
                            soci::use( action.account.to_string() );
                } catch (...) {
                    wlog("${sql}",("sql",insertassets) );
                    wlog( "create asset failed. ${issuer} ${maximum_supply}",("issuer",issuer)("maximum_supply",maximum_supply) );
                }

            } else if( action.name == N(issue) ){

                string to;
                chain::asset quantity;

                try{ 
                    to = abi_data["to"].as<chain::name>().to_string();
                    quantity = abi_data["quantity"].as<chain::asset>();
                } catch(std::exception e) {
                    wlog( "issue args transform variant failed ${account} ${e}",("account",action.account)("e",e.what()) );
                    return ;
                } catch(...) {
                    wlog( "issue args transform vartient failed ${account}",("account",action.account) );
                    return ;
                }
                
                auto symbol_owner = action.account.to_string() + "_" + quantity.get_symbol().name();
                string symbol_owner_account ;
                string issuer;

                try{
                    //update asset issue amount
                    *m_session << "UPDATE assets SET amount = amount + :am WHERE symbol_owner = :so",
                        soci::use( quantity.to_real() ),
                        soci::use( symbol_owner );

                    *m_session << "SELECT issuer FROM assets WHERE symbol_owner = :so",
                        soci::into( issuer ),
                        soci::use( symbol_owner );

                    symbol_owner_account = action.account.to_string() + "_" + issuer + "_" + quantity.get_symbol().name();

                    //add issue's assets and then will have a transfer action to transfer issue's amount to "to".
                    *m_session << "INSERT INTO tokens ( account, symbol, amount, symbol_owner, symbol_owner_account )  VALUES( :ac, :sym, :am, :so, :soac ) "
                            "on  DUPLICATE key UPDATE amount = amount +  :amt ",
                            soci::use( issuer ),
                            soci::use( quantity.get_symbol().name() ),
                            soci::use( quantity.to_real() ),
                            soci::use( symbol_owner ),
                            soci::use( symbol_owner_account ),
                            soci::use( quantity.to_real() );
                } catch(std::exception e) {
                    wlog("my god : ${e}",("e",e.what()));
                } catch(...) {
                    wlog( "INSERT INTO tokens ( account, symbol, amount, symbol_owner, symbol_owner_account )  VALUES( :ac, :sym, :am, :so, :soac ) " );
                    wlog( "issue asset failed. ${issuer} ${quantity}",("issuer",issuer)("quantity",quantity) );
                }

            } else if ( action.name == N(transfer) ){

                string from;
                string to;
                chain::asset quantity;

                try{ 
                    from = abi_data["from"].as<chain::name>().to_string();
                    to = abi_data["to"].as<chain::name>().to_string();
                    quantity = abi_data["quantity"].as<chain::asset>();
                } catch(std::exception e) {
                    wlog( "transfer args transform variant failed ${account } ${e}",("account",action.account)("e",e.what()) );
                    return ;
                } catch(...) {
                    wlog( "transfer args transform vartient failed ${account} ",("account",action.account) );
                    return ;
                }

                auto symbol_owner = action.account.to_string() + "_" + quantity.get_symbol().name();
                auto symbol_owner_account = action.account.to_string() + "_" + to + "_" + quantity.get_symbol().name();
                auto symbol_owner_from = action.account.to_string() + "_" + from + "_" + quantity.get_symbol().name();

                try{
                    *m_session << "INSERT INTO tokens ( account, symbol, amount, symbol_owner, symbol_owner_account )  VALUES( :ac, :sym, :am, :so, :soac ) "
                            "on  DUPLICATE key UPDATE amount = amount +  :amt ",
                            soci::use( to ),
                            soci::use( quantity.get_symbol().name() ),
                            soci::use( quantity.to_real() ),
                            soci::use( symbol_owner ),
                            soci::use( symbol_owner_account ),
                            soci::use( quantity.to_real() );

                    *m_session << "UPDATE tokens SET amount = amount - :am WHERE symbol_owner_account = :soac ",
                            soci::use( quantity.to_real() ),
                            soci::use( symbol_owner_from );

                } catch(std::exception e) {
                    wlog("my god : ${e}",("e",e.what()));
                } catch(...) {
                    wlog( "transfer failed. ${from} transfer to ${to} ${quantity} ",("from",from)("to",to)("quantity",quantity) );
                }

            }
            
        }
        
    }

} // namespace
