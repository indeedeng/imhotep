#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <utility>

#include "merge_iterator.hpp"
#include "mmapped_file.hpp"
#include "term_seq_iterator.hpp"
#include "tgs_op_iterator.hpp"

using namespace std;
using namespace imhotep;

template <typename term_t>
void test_tgs_op_it(const vector<string>& splits) {
    Shard::packed_table_ptr table(0);
    vector<MergeInput<term_t>> inputs;
    vector<shared_ptr<MMappedFile>> split_files;
    for (string split: splits) {
        split_files.push_back(make_shared<MMappedFile>(split));
        SplitView view(split_files.back()->begin(), split_files.back()->end());
        inputs.push_back(MergeInput<term_t>(SplitIterator<term_t>(view), table, 0));
    }

    MergeIterator<term_t>   mit(inputs.begin(), inputs.end());
    MergeIterator<term_t>   mend;
    TermSeqIterator<term_t> sit(mit, mend);
    TermSeqIterator<term_t> send;
    Operation<term_t>       op = Operation<term_t>::field_start(0, "test_field");
    TGSOpIterator<term_t>   it(op, sit, send);
    TGSOpIterator<term_t>   end;
    while (it != end) {
        cout << it->to_string() << endl;
        ++it;
    }
}

int main(int argc, char *argv[])
{
    const string kind(argv[1]);

    vector<string> splits;
    string str;
    while (getline(cin, str) && str.length()) {
        splits.push_back(str);
    }

    if (kind == "int") {
        test_tgs_op_it<IntTerm>(splits);
    }
    else if (kind == "string") {
        test_tgs_op_it<StringTerm>(splits);
    }
    else {
        cerr << "Say what?" << endl;
        exit(1);
    }
}
