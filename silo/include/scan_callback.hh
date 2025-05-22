#pragma once

class TxExecutor;

class TxScanCallback : public MasstreeWrapper<Tuple>::ScanCallback {
public:
    TxExecutor* tx_;

    explicit TxScanCallback(TxExecutor* tx) : tx_(tx) {}

    void on_resp_node(const MasstreeWrapper<Tuple>::node_type* n,
                      uint64_t version) override;

    bool invoke(const std::string_view& k, Tuple v,
                const MasstreeWrapper<Tuple>::node_type* n,
                uint64_t version) override {
        return true;
    }
};
