/**
 * @file tpcc_initializer.hpp
 * @details TPC-C
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf
 * 4.3.3 Table Population Requirements
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <thread>
#include <vector>

#include "../cpu.hh"
#include "../tuple_body.hh"
#include "../workload.hh"

#include "tpcc_common.hh"
#include "tpcc_tables.hh"

// #include "interface.h"
// #include "./index/masstree_beta/include/masstree_beta_wrapper.h"
// #include "tpcc_tables.hpp"
// #include "common.hh"
// #include "../include/random.hh"
// #include "epoch.h"

template <typename Tuple, typename Param>
class TPCCInitializaer {
public:
  static void db_insert_raw([[maybe_unused]] size_t thid, [[maybe_unused]] Param *param,
                    Storage st, std::string_view key, HeapObject&& val) {
      Tuple* tuple = new Tuple();
      tuple->init(thid, TupleBody(key, std::move(val)), param);

      // TODO:
      // rec->set_for_load();

      MasstreeWrapper<Tuple>::thread_init(cached_sched_getcpu());
      Masstrees[get_storage(st)].insert_value(key, tuple);
      // TODO: status handling
      // Status sta = kohler_masstree::insert_record(st, key, rec);
      // if (sta != Status::OK) {
      //     std::cout << __FILE__ << " : " << __LINE__ << " : "
      //               << "fatal error. unique key restriction." << std::endl;
      //     std::cout << "st : " << static_cast<int>(st)
      //               << ", key : " << str_view_hex(key)
      //               << ", val : " << str_view_hex(rec->get_tuple().get_val()) << std::endl;
      //     std::abort();
      // }
  }


  //CREATE Item
  static void load_item([[maybe_unused]] Param* param) {

    struct S {
      static void work([[maybe_unused]] Param* param,
                       std::uint32_t i_id_start, std::uint32_t i_id_end, const IsOriginal& is_original) {
        for (std::uint32_t i_id = i_id_start; i_id <= i_id_end; ++i_id) {
          assert(i_id != 0); // 1-origin
          HeapObject obj;
          obj.allocate<Item>();
          Item& ite = obj.ref();
          ite.I_ID = i_id;
          ite.I_IM_ID = random_int(1, 10000);
          random_alpha_string(14, 24, ite.I_NAME);
          ite.I_PRICE = random_double(100, 10000, 100);
          std::size_t dataLen = random_alpha_string(26, 50, ite.I_DATA);
          if (is_original[i_id - 1]) make_original(ite.I_DATA, dataLen);
#ifdef DEBUG
          std::cout<<"I_ID:"<<ite.I_ID<<"\tI_IM_ID:"<<ite.I_IM_ID<<"\tI_NAME:"<<ite.I_NAME<<"\tI_PRICE:"<<ite.I_PRICE<<"\tI_DATA:"<<ite.I_DATA<<std::endl;
#endif
          SimpleKey<8> key{};
          ite.createKey(key.ptr());
          db_insert_raw(0, param, Storage::Item, key.view(), std::move(obj));
        }
      }
    };

    IsOriginal is_original(MAX_ITEMS, MAX_ITEMS / 10);
#if 0
    std::vector<std::thread> thv;
    /**
     * precondition : para_num > 3
     */
    constexpr std::std::size_t para_num{10};
    thv.emplace_back(S::work, 1, MAX_ITEMS / para_num, std::ref(is_original));
    for (std::std::size_t i = 1; i < para_num - 1; ++i) {
      thv.emplace_back(S::work, (MAX_ITEMS / para_num) * i + 1, (MAX_ITEMS / para_num) * (i + 1), , std::ref(is_original));
    }
    thv.emplace_back(S::work, (MAX_ITEMS / para_num) * (para_num - 1) + 1, MAX_ITEMS, , std::ref(is_original));

    for (auto &&th : thv) {
      th.join();
    }
#else
    // single threaded.
    //::printf("load item...\n");
    S::work(param, 1, MAX_ITEMS, is_original);
    //::printf("load item done\n");
#endif
  }

  //CREATE Warehouses
  static void load_warehouse([[maybe_unused]] Param *param, std::uint16_t w_id) {
    assert(w_id != 0); // 1-origin
    HeapObject obj;
    obj.allocate<Warehouse>();
    Warehouse& ware = obj.ref();
    ware.W_ID = w_id;
    random_alpha_string(6, 10, ware.W_NAME);
    make_address(ware.W_STREET_1,
                ware.W_STREET_2,
                ware.W_CITY,
                ware.W_STATE,
                ware.W_ZIP);
    ware.W_TAX = random_double(0, 2000, 10000);
    ware.W_YTD = 300000;

#ifdef DEBUG
    std::cout<<"W_ID:"<<ware.W_ID<<"\tW_NAME:"<<ware.W_NAME<<"\tW_STREET_1:"<<ware.W_STREET_1<<"\tW_CITY:"<<ware.W_CITY<<"\tW_STATE:"<<ware.W_STATE<<"\tW_ZIP:"<<ware.W_ZIP<<"\tW_TAX:"<<ware.W_TAX<<"\tW_YTD:"<<ware.W_YTD<<std::endl;
#endif
    SimpleKey<8> wh_key{};
    ware.createKey(wh_key.ptr());
    db_insert_raw(w_id, param, Storage::Warehouse, wh_key.view(), std::move(obj));
  }

  //CREATE Stock
  static void load_stock([[maybe_unused]] Param *param, std::uint16_t w_id) {

    struct S {
      static void work([[maybe_unused]] Param *param,
                       std::uint32_t i_id_start, std::uint32_t i_id_end, std::uint16_t w_id, const IsOriginal& is_original) {
        for (std::uint32_t i_id = i_id_start; i_id <= i_id_end; ++i_id) {
          assert(i_id != 0); // 1-origin
          HeapObject obj;
          obj.allocate<Stock>();
          Stock& st = obj.ref();
          st.S_I_ID = i_id;
          st.S_W_ID = w_id;
          st.S_QUANTITY = random_int(10, 100);
          for (char* out : {
              st.S_DIST_01, st.S_DIST_02, st.S_DIST_03, st.S_DIST_04, st.S_DIST_05,
              st.S_DIST_06, st.S_DIST_07, st.S_DIST_08, st.S_DIST_09, st.S_DIST_10}) {
            random_alpha_string(24, 24, out);
          }
          st.S_YTD = 0;
          st.S_ORDER_CNT = 0;
          st.S_REMOTE_CNT = 0;
          std::size_t dataLen = random_alpha_string(26, 50, st.S_DATA);
          if (is_original[i_id - 1]) make_original(st.S_DATA, dataLen);

          SimpleKey<8> st_key{};
          st.createKey(st_key.ptr());
          db_insert_raw(w_id, param, Storage::Stock, st_key.view(), std::move(obj));
        }
      }
    };
    const std::size_t stock_num{MAX_ITEMS};
    IsOriginal is_original(stock_num, stock_num / 10);

#if 0
    constexpr std::std::size_t stock_num_per_thread{5000};
    const std::std::size_t para_num{stock_num / stock_num_per_thread};
    std::vector<std::thread> thv;
    thv.emplace_back(S::work, 1, stock_num_per_thread, w_id, std::ref(is_original));
    for (std::std::size_t i = 1; i < para_num - 1; ++i) {
      thv.emplace_back(S::work, i * stock_num_per_thread + 1, (i + 1) * stock_num_per_thread, w_id, , std::ref(is_original));
    }
    thv.emplace_back(S::work, (para_num - 1) * stock_num_per_thread + 1, stock_num, w_id, , std::ref(is_original));

    for (auto &&th : thv) {
      th.join();
    }
#else
    // single-threaded
    S::work(param, 1, stock_num, w_id, is_original);
#endif
  }

  //CREATE History
  static void load_history([[maybe_unused]] Param *param,
                           std::uint16_t w_id, uint8_t d_id, std::uint32_t c_id, std::string_view key) {
    std::time_t now = get_lightweight_timestamp();
    HeapObject obj;
    obj.allocate<History>();
    History& history = obj.ref();
    history.H_C_ID = c_id;
    history.H_C_D_ID = history.H_D_ID = d_id;
    history.H_C_W_ID = history.H_W_ID = w_id;
    history.H_DATE = now;
    history.H_AMOUNT = 10.00;
    random_alpha_string(12, 24, history.H_DATA);

    db_insert_raw(w_id, param, Storage::History, key, std::move(obj));
  }

  //CREATE Orderline
  static void load_orderline([[maybe_unused]] Param *param,
                             std::uint16_t w_id, std::uint16_t d_id, std::uint32_t o_id, uint8_t ol_num) {
    std::time_t now = get_lightweight_timestamp();
    HeapObject obj;
    obj.allocate<OrderLine>();
    OrderLine& order_line = obj.ref();
    order_line.OL_O_ID = o_id;
    order_line.OL_D_ID = d_id;
    order_line.OL_W_ID = w_id;
    order_line.OL_NUMBER = ol_num;
    order_line.OL_I_ID = random_int(1, 100000);
    order_line.OL_SUPPLY_W_ID = w_id;
    if (order_line.OL_O_ID < 2101) {
      order_line.OL_DELIVERY_D = now;
    } else {
      order_line.OL_DELIVERY_D = 0;
    }
    order_line.OL_QUANTITY = 5;
    if (order_line.OL_O_ID < 2101) {
      order_line.OL_AMOUNT = 0.00;
    } else {
      order_line.OL_AMOUNT = random_double(1, 999999, 100);
    }
    random_alpha_string(24, 24, order_line.OL_DIST_INFO);

    SimpleKey<8> key{};
    order_line.createKey(key.ptr());
    db_insert_raw(w_id, param, Storage::OrderLine, key.view(), std::move(obj));
  }

  //CREATE Order
  static void load_order([[maybe_unused]] Param *param,
                         std::uint16_t w_id, uint8_t d_id, std::uint32_t o_id, std::uint32_t c_id) {
    std::time_t now = get_lightweight_timestamp();
    HeapObject obj;
    obj.allocate<Order>();
    Order& order = obj.ref();
    order.O_ID = o_id;
    order.O_C_ID = c_id;
    order.O_D_ID = d_id;
    order.O_W_ID = w_id;
    order.O_ENTRY_D = now;
    if (order.O_ID < 2101) {
      order.O_CARRIER_ID = random_int(1, 10);
    } else {
      order.O_CARRIER_ID = 0;
    }
    order.O_OL_CNT = random_int(5, 15);
    order.O_ALL_LOCAL = 1;

    {
      HeapObject key_obj;
      key_obj.allocate<SimpleKey<8>>();
      SimpleKey<8>& key = key_obj.ref();
      order.createKey(key.ptr());
      db_insert_raw(w_id, param, Storage::Order, key.view(), std::move(obj));

      char o_secondary_key_buf[16];
      std::string_view o_secondary_key = order.createSecondaryKey(&o_secondary_key_buf[0]);
      // ::printf("o_cust_key %s\n", str_view_hex(o_cust_key).c_str());

      // TODO: consider to store o_id directly as value of masstree
      db_insert_raw(w_id, param, Storage::OrderSecondary, o_secondary_key, std::move(key_obj));
    }
    //O_OL_CNT orderlines per order.
    for (uint8_t ol_num = 1; ol_num <= order.O_OL_CNT + 1; ol_num++) {
      load_orderline(param, w_id, d_id, o_id, ol_num);
    }

    //CREATE NewOrder 900 rows
    if (2100 < c_id) {
      HeapObject obj;
      obj.allocate<NewOrder>();
      NewOrder& new_order = obj.ref();
      new_order.NO_O_ID = o_id;
      new_order.NO_D_ID = d_id;
      new_order.NO_W_ID = w_id;
      {
        SimpleKey<8> key{};
        new_order.createKey(key.ptr());
        db_insert_raw(w_id, param, Storage::NewOrder, key.view(), std::move(obj));
      }
    }
  }


  //CREATE Customer
  static void load_customer([[maybe_unused]] Param *param,
                            uint8_t d_id, std::uint16_t w_id, HistoryKeyGenerator &hkg) {
    struct S {
      static void
      work([[maybe_unused]] Param *param,
           std::uint32_t c_id_start, std::uint32_t c_id_end, HistoryKeyGenerator &hkg,
          uint8_t d_id, std::uint16_t w_id, const Permutation& perm) {
        for (std::uint32_t c_id = c_id_start; c_id <= c_id_end; ++c_id) {
          assert(c_id != 0); // 1-origin.
          std::time_t now = get_lightweight_timestamp();
          HeapObject obj;
          obj.allocate<Customer>();
          Customer& customer = obj.ref();
          customer.C_ID = c_id;
          customer.C_D_ID = d_id;
          customer.C_W_ID = w_id;
          if (c_id <= 1000) {
            // for all c_last patterns [0, 999] to be exist.
            make_c_last(c_id - 1, customer.C_LAST);
  #ifdef DEBUG
            std::cout<<"C_LAST:"<<customer.C_LAST<<std::endl;
  #endif
          } else {
            make_c_last(non_uniform_random<255, true>(0, 999), customer.C_LAST);
          }
          copy_cstr(customer.C_MIDDLE, "OE", sizeof(customer.C_MIDDLE));
          random_alpha_string(8, 16, customer.C_FIRST);
          make_address(customer.C_STREET_1,
                      customer.C_STREET_2,
                      customer.C_CITY,
                      customer.C_STATE,
                      customer.C_ZIP);
          random_number_string(16, 16, customer.C_PHONE);

  #ifdef DEBUG
          if(c==start&& w==1&& d==2)std::cout<<"C_PHONE:"<<customer.C_PHONE<<std::endl;
  #endif
          customer.C_SINCE = now;
          //90% GC 10% BC
          if (random_int(0, 99) < 10) {
            copy_cstr(customer.C_CREDIT, "BC", 3);
          } else {
            copy_cstr(customer.C_CREDIT, "GC", 3);
          }
          customer.C_CREDIT_LIM = 50000.00;
          customer.C_DISCOUNT = random_double(0, 5000, 10000);
          customer.C_BALANCE = -10.00;
          customer.C_YTD_PAYMENT = 10.00;
          customer.C_PAYMENT_CNT = 1;
          customer.C_DELIVERY_CNT = 0;
          random_alpha_string(300, 500, customer.C_DATA);

          SimpleKey<8> pkey{};
          customer.createKey(pkey.ptr());
          char c_last_key_buf[Customer::CLastKey::required_size()];
          std::string_view c_last_key = customer.createSecondaryKey(&c_last_key_buf[0]);
          // ::printf("c_last_key %s\n", str_view_hex(c_last_key).c_str());

          db_insert_raw(w_id, param, Storage::Customer, pkey.view(), std::move(obj));

          std::vector<SimpleKey<8>> *ctn_ptr;
          MasstreeWrapper<Tuple>::thread_init(cached_sched_getcpu());
          Tuple* tuple = Masstrees[get_storage(Storage::CustomerSecondary)].get_value(c_last_key);
          if (tuple != nullptr) {
            memcpy(&ctn_ptr, tuple->body_.get_val().data(), sizeof(uintptr_t));
            //::printf("found %p\n", ctn_ptr);
            ctn_ptr->push_back(pkey);
          } else {
            ctn_ptr = new std::vector<SimpleKey<8>>;
            ctn_ptr->reserve(8); // 8 * 8 = 64 bytes.
            //::printf("new   %p\n", ctn_ptr);
            ctn_ptr->push_back(pkey);
            HeapObject obj;
            obj.allocate<uintptr_t>();
            uintptr_t& p = obj.ref();
            p = uintptr_t(ctn_ptr);
            db_insert_raw(w_id, param, Storage::CustomerSecondary, c_last_key, std::move(obj));
          }

          struct S {
            static Customer *search(const SimpleKey<8> &pkey) {
              MasstreeWrapper<Tuple>::thread_init(cached_sched_getcpu());
              auto *tuple = reinterpret_cast<Tuple*>(Masstrees[get_storage(Storage::Customer)].get_value(pkey.view()));
              return reinterpret_cast<Customer *>(const_cast<char *>(tuple->body_.get_val().data()));
            }

            static bool less(const SimpleKey<8> &lh, const SimpleKey<8> &rh) {
              const Customer *lh_cust = search(lh);
              const Customer *rh_cust = search(rh);
              return ::strncmp(lh_cust->C_FIRST, rh_cust->C_FIRST, sizeof(Customer::C_FIRST)) < 0;
            }
          };

          std::sort(ctn_ptr->begin(), ctn_ptr->end(), S::less);
          //1 histories per customer.
          SimpleKey<8> his_key = hkg.get_as_simple_key();
          load_history(param, w_id, d_id, c_id, his_key.view());
          //1 order per customer.
          std::uint32_t o_id = c_id;
          load_order(param, w_id, d_id, o_id, perm[c_id - 1]);
        }
      }
    };

    Permutation perm(1, CUST_PER_DIST);
    S::work(param, 1, CUST_PER_DIST, hkg, d_id, w_id, perm);

  #if 0
    constexpr std::std::size_t cust_num_per_th{500};
    constexpr std::std::size_t para_num{CUST_PER_DIST / cust_num_per_th};
    std::vector<std::thread> thv;
    thv.emplace_back(S::work, 1, cust_num_per_th, std::ref(hkg), d, w, std::ref(perm));
    for (std::std::size_t i = 1; i < para_num - 1; ++i) {
      thv.emplace_back(S::work, i * cust_num_per_th + 1, (i + 1) * cust_num_per_th, std::ref(hkg), d, w, , std::ref(perm));
    }
    thv.emplace_back(S::work, (para_num - 1) * cust_num_per_th + 1, CUST_PER_DIST, std::ref(hkg), d, w, , std::ref(perm));

    for (auto &&th : thv) {
      th.join();
    }
  #endif
  }

  static void load_district([[maybe_unused]] Param *param, std::uint16_t w_id) {
    struct S {
      static void work([[maybe_unused]] Param *param,
                       uint8_t d_id, std::uint16_t w_id, HistoryKeyGenerator &hkg) {
        assert(d_id != 0); // 1-origin.
        HeapObject obj;
        obj.allocate<District>();
        District& district = obj.ref();
        district.D_ID = d_id;
        district.D_W_ID = w_id;
        random_alpha_string(6, 10, district.D_NAME);
        make_address(district.D_STREET_1,
                    district.D_STREET_2,
                    district.D_CITY,
                    district.D_STATE,
                    district.D_ZIP);
        district.D_TAX = random_double(0, 2000, 10000);
        district.D_YTD = 30000.00;
        district.D_NEXT_O_ID = 3001;

  #ifdef DEBUG
        std::cout<<"D_ID:"<<district.D_ID<<std::endl;
  #endif
        SimpleKey<8> key{};
        district.createKey(key.ptr());
        db_insert_raw(w_id, param, Storage::District, key.view(), std::move(obj));

        // CREATE Customer History Order Orderline. 3000 customers per a district.
        load_customer(param, d_id, w_id, hkg);
      }
    };
    HistoryKeyGenerator hkg{};
    assert(w_id != 0); // 1-origin.
    hkg.init(w_id - 1, false);

  #if 0
    std::vector<std::thread> thv;
    for (std::size_t d = 1; d <= DIST_PER_WARE; ++d) {
      thv.emplace_back(S::work, d, w, std::ref(hkg));
    }
    for (auto &&th : thv) {
      th.join();
    }
  #else
    // single-threaded.
    for (uint8_t d_id = 1; d_id <= DIST_PER_WARE; ++d_id) {
      S::work(param, d_id, w_id, hkg);
    }
  #endif

  }

  static void load(Param* param) {
    //ID 1-origin

    std::vector<std::thread> thv;
    std::cout << "[start] load." << std::endl;

    thv.emplace_back(load_item, param);
    for (std::size_t w = 1; w <= FLAGS_tpcc_num_wh; ++w) {
      thv.emplace_back(load_warehouse, param, w);
      //100,000 stocks per warehouse
      thv.emplace_back(load_stock, param, w);
      //10 districts per warehouse
      thv.emplace_back(load_district, param, w);
    }

    for (auto &&th : thv) {
      th.join();
    }
    std::cout << "[end] load." << std::endl;
  }
};
