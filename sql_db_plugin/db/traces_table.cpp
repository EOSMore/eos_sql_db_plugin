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
                //parse_actions(atc.act);
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

        
    }

} // namespace
