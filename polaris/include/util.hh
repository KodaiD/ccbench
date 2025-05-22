#pragma once

extern void chkArg();

extern bool chkEpochLoaded();

extern void displayDB();

extern void displayParameter();

extern void genLogFile(std::string& logpath, const int thid);

extern void siloLeaderWork(uint64_t& epoch_timer_start,
                           uint64_t& epoch_timer_stop);

extern void ShowOptParameters();

class DefaultInitializer {
public:
    DefaultInitializer() {}
    void makeDB();
};
