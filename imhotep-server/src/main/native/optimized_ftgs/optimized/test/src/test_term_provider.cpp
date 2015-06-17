#include <iostream>
#include <thread>
#include <utility>

#include "executor_service.hpp"
#include "shard.hpp"
#include "tgs_op_iterator.hpp"
#include "term_provider.hpp"

using namespace std;
using namespace imhotep;

template <typename term_t>
TermProvider<term_t> make_provider(ExecutorService&     executor,
                                   const vector<Shard>& shards,
                                   const string&        field,
                                   const string&        split_dir,
                                   size_t num_splits) {
    vector<typename TermProvider<term_t>::term_source_t> sources;
    for (const Shard& shard: shards) {
        TermIterator<term_t> it(shard, field);
        sources.push_back(make_pair(shard, it));
    }

    return TermProvider<term_t>(sources, field, split_dir, num_splits, executor);
}

template <typename term_t>
void test_term_provider(ExecutorService&      executor,
                        TermProvider<term_t>& provider,
                        const string&         field,
                        size_t num_splits) {
    for (size_t split_num(0); split_num < num_splits; ++split_num) {
        // executor.enqueue([&provider, field, split_num] {
                size_t                  num_term_seqs(0);
                TermSeqIterator<term_t> term_seq_it(provider.term_seq_it(split_num));
                TermSeqIterator<term_t> term_seq_end;
                while (term_seq_it != term_seq_end) {
                    cout << term_seq_it->to_string() << endl;
                    ++num_term_seqs;
                    ++term_seq_it;
                }
                cout << "num_term_seqs: " << num_term_seqs << endl;
            // });
    }
    // executor.await_completion();
}

template <typename term_t>
void test_tgs_ops(ExecutorService&      executor,
                  TermProvider<term_t>& provider,
                  const string&         field,
                  size_t num_splits) {
    for (size_t split_num(0); split_num < num_splits; ++split_num) {
        // executor.enqueue([&provider, field, split_num] {
                size_t                  num_ops(0);
                Operation<term_t>       op = Operation<term_t>::field_start(split_num, field);
                TermSeqIterator<term_t> term_seq_begin(provider.term_seq_it(split_num));
                TermSeqIterator<term_t> term_seq_end;
                TGSOpIterator<term_t>   it(op, term_seq_begin, term_seq_end);
                TGSOpIterator<term_t>   end;
                while (it != end) {
                    cout << it->to_string() << endl;
                    ++num_ops;
                    ++it;
                }
                cout << "num_ops: " << num_ops << endl;
            // });
    }
    // executor.await_completion();
}

int main(int argc, char *argv[])
{
    const string kind(argv[1]);
    const string field(argv[2]);
    const string split_dir(argv[3]);

    vector<string> int_fields;
    vector<string> str_fields;
    if (kind == "int") {
        int_fields.push_back(field);
    }
    else if (kind == "string") {
        str_fields.push_back(field);
    }

    vector<Shard> shards;
    string str;
    while (getline(cin, str) && str.length()) {
        shards.push_back(Shard(str, int_fields, str_fields));
    }

    //    static constexpr size_t num_splits = 7;
    static constexpr size_t num_splits = 3;
    ExecutorService executor;

    if (kind == "int") {
        TermProvider<IntTerm> provider(make_provider<IntTerm>(executor, shards, field, split_dir, num_splits));
        test_term_provider<IntTerm>(executor, provider, field, num_splits);
        // test_tgs_ops<IntTerm>(executor, provider, field, num_splits);
    }
    else if (kind == "string") {
        TermProvider<StringTerm> provider(make_provider<StringTerm>(executor, shards, field, split_dir, num_splits));
        // test_term_provider<StringTerm>(executor, provider, field, num_splits);
        test_tgs_ops<StringTerm>(executor, provider, field, num_splits);
    }
    else {
        cerr << "Say what?" << endl;
        exit(1);
    }
}
