#include "types.h"
#include "kernel.hh"
#include "fs.h"
#include "file.hh"
#include "mnode.hh"
#include "mfs.hh"
#include "scalefs.hh"
#include "kstream.hh"
#include "major.h"


// Applies all metadata operations logged in the logical log. Called on sync.
void
mfs_interface::process_metadata_log()
{
  mfs_operation_vec ops;
  u64 sync_tsc = get_tsc();

  {
    auto guard = metadata_log->synchronize_upto_tsc(sync_tsc);
    for (auto it = metadata_log->operation_vec.begin(); it !=
      metadata_log->operation_vec.end(); it++)
      ops.push_back(*it);
    metadata_log->operation_vec.clear();
  }

  // If we find create, link, unlink, rename and delete for the same file,
  // absorb all of them and discard those transactions, since the delete
  // cancels out everything else.

  prune_trans_log = new linearhash<u64, mfs_op_idx>(ops.size()*5);
  std::vector<unsigned long> erase_indices;

  // TODO: Handle the scenario where the transaction log contains delete,
  // create and delete for the same mnode.

  for (auto it = ops.begin(); it != ops.end(); ) {
    auto mfs_op_create = dynamic_cast<mfs_operation_create*>(*it);
    auto mfs_op_link   = dynamic_cast<mfs_operation_link*>(*it);
    auto mfs_op_unlink = dynamic_cast<mfs_operation_unlink*>(*it);
    auto mfs_op_rename = dynamic_cast<mfs_operation_rename*>(*it);
    auto mfs_op_delete = dynamic_cast<mfs_operation_delete*>(*it);

    if (mfs_op_create) {

      mfs_op_idx m, mptr;
      m.create_index = it - ops.begin();

      if (prune_trans_log->lookup(mfs_op_create->mnode, &mptr)) {
        panic("process_metadata_log: multiple creates for the same mnode!\n");
      }

      prune_trans_log->insert(mfs_op_create->mnode, m);
      it++;

    } else if (mfs_op_link) {

      mfs_op_idx m, mptr;
      if (prune_trans_log->lookup(mfs_op_link->mnode, &mptr)) {
        prune_trans_log->remove(mfs_op_link->mnode);
        m = mptr;
        m.link_index = it - ops.begin();
      } else {
        m.link_index = it - ops.begin();
      }

      prune_trans_log->insert(mfs_op_link->mnode, m);
      it++;

    } else if (mfs_op_unlink) {

      mfs_op_idx m, mptr;
      if (prune_trans_log->lookup(mfs_op_unlink->mnode, &mptr)) {
        prune_trans_log->remove(mfs_op_unlink->mnode);
        m = mptr;
        m.unlink_index = it - ops.begin();
      } else {
        m.unlink_index = it - ops.begin();
      }

      prune_trans_log->insert(mfs_op_unlink->mnode, m);
      it++;

    } else if (mfs_op_rename) {

      mfs_op_idx m, mptr;
      if (prune_trans_log->lookup(mfs_op_rename->mnode, &mptr)) {
        prune_trans_log->remove(mfs_op_rename->mnode);
        m = mptr;
        m.rename_index = it - ops.begin();
      } else {
        m.rename_index = it - ops.begin();
      }

      prune_trans_log->insert(mfs_op_rename->mnode, m);
      it++;

    } else if (mfs_op_delete) {

      mfs_op_idx m, mptr;
      if (prune_trans_log->lookup(mfs_op_delete->mnode, &mptr)) {
        prune_trans_log->remove(mfs_op_delete->mnode);
        m = mptr;

        m.delete_index = it - ops.begin();

        // Absorb only if the corresponding create is also found.
        if (m.create_index != -1) {
          erase_indices.push_back(m.create_index);

          if (m.link_index != -1)
            erase_indices.push_back(m.link_index);

          if (m.unlink_index != -1)
            erase_indices.push_back(m.unlink_index);

          if (m.rename_index != -1)
            erase_indices.push_back(m.rename_index);

          erase_indices.push_back(m.delete_index);

          it++;

        } else {
          // If we didn't find the create, we should execute all the
          // transactions.
          it++;
        }

      } else {

        m.delete_index = it - ops.begin();
        prune_trans_log->insert(mfs_op_delete->mnode, m);
        it++;
      }

    }
  }

  std::sort(erase_indices.begin(), erase_indices.end(),
            std::greater<unsigned long>());

  for (auto &idx : erase_indices)
    ops.erase(ops.begin() + idx);

  delete prune_trans_log;

  for (auto it = ops.begin(); it != ops.end(); it++) {
    transaction *tr = new transaction((*it)->timestamp);
    (*it)->apply(tr);
    add_to_journal_locked(tr);
    delete (*it);
  }
}
