//DISPLAY_GLOBAL_CC
#include <bitset>
#include <iomanip>
#include <limits>
#include "include/common.hpp"
#include "include/debug.hpp"

using namespace std;

void 
displayDB() 
{

	Tuple *tuple;

	for (unsigned int i = 0; i < TUPLE_NUM; ++i) {
		tuple = &Table[i];
		cout << "------------------------------" << endl;	//-は30個
		cout << "key: " << tuple->key << endl;
		cout << "val: " << tuple->val << endl;
		cout << "TS_word: " << tuple->tsword.load() << endl;
		cout << "bit: " << static_cast<bitset<64>>(tuple->tsword.load()) << endl;
		cout << endl;
	}
}

void 
displayPRO() 
{
   	for (unsigned int i = 0; i < PRO_NUM; i++) { 
		cout << "transaction No." << i << endl;
		for (unsigned int j = 0; j < MAX_OPE; j++) {
	   		cout << "(ope, key, val) = (";
			switch(Pro[i][j].ope){
				case Ope::READ:
					cout << "READ";
					break;
				case Ope::WRITE:
					cout << "WRITE";
					break;
				default:
					break;
			}
   			cout << ", " << Pro[i][j].key
   			<< ", " << Pro[i][j].val << ")" << endl;
		}
	}
}

void
displayTransactionRange()
{
	cout << "display TransactionRange()" << endl;
	for (unsigned int i = 1; i < THREAD_NUM; ++i) {
		cout << "th #" << i << ": " << PRO_NUM / (THREAD_NUM - 1) * (i - 1) << ", " << PRO_NUM / (THREAD_NUM - 1) * (i) - 1 << endl;
	}
	cout << endl;
}

void
displayFinishTransactions()
{
	cout << "display FinishTransactions" << endl;
	for (unsigned int i = 0; i < THREAD_NUM; ++i) {
		cout << "th #" << i << ": " << FinishTransactions[i].num << endl;
	}
	cout << endl;
}

void
displayAbortCounts()
{
	cout << "display AbortCounts()" << endl;
	for (unsigned int i = 0; i < THREAD_NUM; ++i) {
		cout << "th #" << i << ": " << AbortCounts[i].num << endl;
	}
	cout << endl;
}

void 
displayAbortRate() 
{
	long double sumT(0), sumA(0);
	long double rate[THREAD_NUM] = {};
	for (unsigned int i = 1; i < THREAD_NUM; ++i) {
		sumT += FinishTransactions[i].num;
		sumA += AbortCounts[i].num;
		rate[i] = sumA / (sumT + sumA);
	}

	long double ave_rate(0);
	for (unsigned int i = 0; i < THREAD_NUM; ++i) {
		ave_rate += rate[i];
	}
	ave_rate /= (long double) THREAD_NUM;

	cout << ave_rate << endl;
}

