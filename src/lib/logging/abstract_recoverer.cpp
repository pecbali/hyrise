#include "abstract_recoverer.hpp"

#include "concurrency/transaction_manager.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

void AbstractRecoverer::_redo_transactions(const TransactionID transaction_id,
                                           std::vector<LoggedItem>& transactions) {
  for (auto& transaction : transactions) {
    if (transaction.transaction_id != transaction_id) continue;

    auto& table = *StorageManager::get().get_table(transaction.table_name);
    auto& chunk = *table.get_chunk(transaction.row_id.chunk_id);

    switch (transaction.type) {
      case LogType::Value: {
        chunk.append(*transaction.values);

        DebugAssert(chunk.has_mvcc_columns(), "Recovery: Table should have MVCC columns.");
        auto mvcc_columns = chunk.mvcc_columns();
        DebugAssert(mvcc_columns->begin_cids.size() - 1 == transaction.row_id.chunk_offset,
                    "recovery rowID " + std::to_string(mvcc_columns->begin_cids.size() - 1) + " != logged rowID " +
                        std::to_string(transaction.row_id.chunk_offset));
        mvcc_columns->begin_cids[mvcc_columns->begin_cids.size() - 1] = 0;
        break;
      }
      case LogType::Invalidation: {
        auto mvcc_columns = chunk.mvcc_columns();
        mvcc_columns->end_cids[transaction.row_id.chunk_offset] = 0;
        break;
      }
      default:
        throw("Recovery: Transaction type not implemented.");
    }
  }

  transactions.erase(std::remove_if(transactions.begin(), transactions.end(),
                                    [&transaction_id](LoggedItem x) { return x.transaction_id == transaction_id; }),
                     transactions.end());
}

void AbstractRecoverer::_recover_table(const std::string& path, const std::string& table_name) {
  auto table = load_table(path, Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, table);
  ++_number_of_loaded_tables;
}

}  // namespace opossum
