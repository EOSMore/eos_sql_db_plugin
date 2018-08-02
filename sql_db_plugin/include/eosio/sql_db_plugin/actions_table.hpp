#pragma once

#include <eosio/sql_db_plugin/table.hpp>


#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include <eosio/chain/block_state.hpp>
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/abi_def.hpp>
#include <eosio/chain/asset.hpp>
#include <eosio/chain/abi_serializer.hpp>

namespace eosio {

using std::string;

struct system_contract_arg{
    system_contract_arg() = default;
    system_contract_arg(const chain::account_name& to, const chain::account_name& from, const chain::asset& quantity, const chain::account_name& receiver, const chain::account_name& payer, const chain::account_name& name)
    :to(to), from(from), quantity(quantity), receiver(receiver), payer(payer), name(name)
    {}
    chain::account_name to;
    chain::account_name from;
    chain::asset quantity;
    chain::account_name receiver;
    chain::account_name payer;
    chain::account_name name;
    chain::account_name account;
};

class actions_table : public mysql_table {
    public:
        actions_table(){}
        actions_table(std::shared_ptr<soci::session> session);

        void drop();
        void create();
        void add(chain::action action, chain::transaction_id_type transaction_id, fc::time_point_sec transaction_time, uint8_t seq); 
        string add_data(chain::action );

        static const chain::account_name newaccount;
        static const chain::account_name setabi;

    private:
        std::shared_ptr<soci::session> m_session;

        void parse_actions(chain::action action);
};


} // namespace
FC_REFLECT( eosio::system_contract_arg                        , (to)(from)(receiver)(payer)(name)(account) )



