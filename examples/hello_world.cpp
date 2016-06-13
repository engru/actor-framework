#include <string>
#include <iostream>

#include "caf/all.hpp"

using std::cout;
using std::endl;
using std::string;


namespace {

using namespace caf;

struct source_state {
  int x = 0;
};

behavior source(stateful_actor<source_state>* self, actor target,
                size_t num_msgs) {
  auto sh = self->new_stream(target, [=]() -> result<int> {
    auto x = self->state.x++;
    if (x == num_msgs) {
      self->quit();
      return skip();
    }
    return x;
  });
  return {[=] {
    // dummy
  }};
}

struct sink_state {
  static const char* name;
  size_t received_messages = 0;
  ~sink_state() {
    printf("received messages: %d\n", (int)received_messages);
  }
};

const char* sink_state::name = "foobar-sink";

behavior sink(stateful_actor<sink_state>* self) {
  return {
    [=](int) {
      if (++self->state.received_messages % 10000 == 0) {
        printf("~~~ received messages: %d\n",
               (int) self->state.received_messages);
      }
    }
  };
}

behavior forward_stage(event_based_actor*) {
  return {
    [=](int x) {
      return x;
    }
  };
}

class config : public actor_system_config {
public:
  size_t num_sources = 20;
  size_t num_msgs = 1000;

  config() {
    opt_group{custom_options_, "global"}
    .add(num_sources, "num-sources,s", "nr. of sources")
    .add(num_msgs, "num-messages,n", "nr. of messages");
  }
};

template <class T>
std::ostream& operator<<(std::ostream& x, const std::vector<T>& ys) {
  return x << deep_to_string(ys);
}

template <class T>
std::ostream& operator<<(std::ostream& x, const optional<T>& ys) {
  return x << to_string(ys);
}

#define CAF_STR(s) #s

#define CAF_XSTR(s) CAF_STR(s)

#define SHOW(x) cout << CAF_XSTR(x) << typeid(x).name() << endl

/// A basic stream is a stream that accepts a consumer
template <class T>
class basic_stream {
public:
  basic_stream(std::function<void (T)> consumer) : consumer_(std::move(consumer)) {
    // nop
  }

  inline size_t open_credit() const {
    return open_credit_;
  }

  // called whenever new credit is available
  /// Returns `false` if the stream reached its end.
  virtual bool consume(size_t up_to) = 0;

private:
  size_t open_credit_;
  std::function<void (T)> consumer_;
};

namespace streaming {

template <class T>
class source : public ref_counted {
public:
  virtual optional<T> pull() = 0;
};

template <class T>
using source_ptr = intrusive_ptr<source<T>>;

template <class T>
class sink {
public:
  virtual ~sink(){
    // nop
  }

  virtual void push(T) = 0;
};

template <class T>
class origin : public sink<T>, public source<T> {};

template <class T>
using origin_ptr = intrusive_ptr<origin<T>>;

template <class In, class Out>
class stage : public source<Out> {
public:
  using prev_stage = source_ptr<In>;

  stage(prev_stage prev) : prev_(prev) {
    // nop
  }

  source<In>& prev() const {
    return *prev_;
  }

private:
  prev_stage prev_;
};

template <class In, class Out>
using stage_ptr = intrusive_ptr<stage<In, Out>>;

// origin -> stage_0 -> ... -> stage_n-1 -> stage_n

template <class F>
using return_type_t = typename detail::get_callable_trait<F>::result_type;

template <class F>
using argument_type_t = typename detail::tl_head<typename detail::get_callable_trait<F>::arg_types>::type;

template <class F>
class mapping_stage : public stage<argument_type_t<F>, return_type_t<F>> {
public:
  using super = stage<argument_type_t<F>, return_type_t<F>>;

  using prev_stage = typename super::prev_state;

  mapping_stage(F fun, prev_stage* prev) : super(prev), fun_(std::move(fun)) {
    // nop
  }

  optional<return_type_t<F>> pull() override {
    auto x = this->prev()->pull();
    if (!x)
      return none;
    return fun_(std::move(*x));
  }

private:
  F fun_;
};

template <class Out>
class consumer : public ref_counted {
public:
  virtual void consume(std::vector<Out>& buf) = 0;

  virtual void shutdown() {
    // nop
  }
};

template <class T>
using consumer_ptr = intrusive_ptr<consumer<T>>;

/// A channel is a DAG with a single root and a single end.
template <class In, class Out>
class pipeline : public ref_counted {
public:
  pipeline(origin_ptr<In> root, source_ptr<Out> endpoint, consumer_ptr<Out> f)
      : closed_(false),
        open_credit_(0),
        v0_(std::move(root)),
        vn_(std::move(endpoint)),
        f_(std::move(f)) {
    // nop
  }

  /// Pushes a single element to the channel.
  template <class Iterator, class Sentinel>
  void push(Iterator i, Sentinel e) {
    CAF_ASSERT(!closed_);
    if (i == e)
      return;
    do {
      v0_->push(*i);
    } while (++i != e);
    if (open_credit_ > 0)
      trigger_consumer();
  }

  void push(std::initializer_list<In> xs) {
    push(xs.begin(), xs.end());
  }

  /// Pushes a single element to the channel.
  void push(In x) {
    CAF_ASSERT(!closed_);
    v0_->push(std::move(x));
    if (open_credit_ > 0)
      trigger_consumer();
  }

  /// Closes the write end of the channel.
  void close() {
    closed_ = true;
  }

  /// Allows the consumer to eat up `x` more items.
  void grant_credit(size_t x) {
    CAF_ASSERT(x > 0);
    open_credit_ += x;
    trigger_consumer();
  }

  size_t open_credit() {
    return open_credit_;
  }

private:
  void trigger_consumer() {
    size_t i = 0;
    for (; i < open_credit_; ++i) {
      auto x = vn_->pull();
      if (x)
        buf_.emplace_back(std::move(*x));
      else
        break;
    }
    if (i > 0) {
      f_->consume(buf_);
      buf_.clear();
    }
    // we can shutdown the consumer if the channel is closed and we could not
    // produce a sufficient number of items, because no new elements will appear
    if (i < open_credit_ && closed_)
      f_->shutdown();
    open_credit_ -= i;
  }

  bool closed_;
  size_t open_credit_;

  origin_ptr<In> v0_;
  source_ptr<Out> vn_;
  consumer_ptr<Out> f_;
  std::vector<Out> buf_;
};

template <class F>
using signature_t = typename detail::get_callable_trait<F>::fun_sig;

template <class T, class S = signature_t<T>>
struct arg_type;

template <class T, class B, class A>
struct arg_type<T, B(A)> {
  using type = A;
};

template <class T>
using arg_type_t = typename arg_type<T>::type;

template <class T, class S = signature_t<T>>
struct result_type;

template <class T, class B, class... As>
struct result_type<T, B(As...)> {
  using type = B;
};

template <class T>
using result_type_t = typename result_type<T>::type;

template <class F, class S = signature_t<F>>
struct is_map_fun : std::false_type {};

template <class F, class R, class T>
struct is_map_fun<F, R (T)> : std::true_type {};

template <class F, class T>
struct is_map_fun<F, bool (T)> : std::false_type {};

template <class F, class S = signature_t<F>>
struct is_filter_fun : std::false_type {};

template <class F, class T>
struct is_filter_fun<F, bool (T)> : std::true_type {};

template <class F, class S = signature_t<F>>
struct is_reduce_fun : std::false_type {};

template <class F, class B, class A>
struct is_reduce_fun<F, void (B&, A)> : std::true_type {};

template <class F, class S = signature_t<F>>
struct first_arg_type;

template <class F, class R, class T, class... Ts>
struct first_arg_type<F, R (T, Ts...)> {
  using type = T;
};

template <class F>
using first_arg_type_t = typename first_arg_type<F>::type;

template <class F, class S = signature_t<F>>
struct second_arg_type;

template <class F, class R, class _, class T, class... Ts>
struct second_arg_type<F, R (_, T, Ts...)> {
  using type = T;
};

template <class F>
using second_arg_type_t = typename second_arg_type<F>::type;

template <class In, class Out, class... Fs>
struct stream_builder {
  std::tuple<Fs...> fs;
};

template <class T, class Out>
struct eval_helper {
public:
  eval_helper(T& x) : x_(x) {
    // nop
  }

  template <class... Fs>
  optional<Out> operator()(Fs&... fs) {
    return apply(x_, fs...);
  }

private:
  optional<Out> apply(Out& x) {
    return std::move(x);
  }

  template <class X, class F, class... Fs>
  typename std::enable_if<is_filter_fun<F>::value, optional<Out>>::type
  apply(X& x, F& f, Fs&... fs) {
    if (!f(x))
      return none;
    return apply(x, fs...);
  }

  template <class X, class F, class... Fs>
  typename std::enable_if<is_map_fun<F>::value, optional<Out>>::type
  apply(X& x, F& f, Fs&... fs) {
    auto y = f(std::move(x));
    return apply(y, fs...);
  }

  T& x_;
};

template <class In, class Out, class... Fs>
optional<Out> eval(stream_builder<In, Out, Fs...> sb, In x) {
  using namespace detail;
  eval_helper<In, Out> h{x};
  return apply_args(h, typename il_range<0, sizeof...(Fs)>::type{}, sb.fs);
}

template <class In, class Out, class... Ts, class F,
          class E = typename std::enable_if<is_filter_fun<F>::value>::type>
stream_builder<In, Out, Ts..., F>
operator|(stream_builder<In, Out, Ts...> x, F f) {
  static_assert(std::is_same<Out, arg_type_t<F>>::value,
                "invalid filter signature");
  return {std::tuple_cat(std::move(x.fs), std::make_tuple(f))};
}

template <class In, class Out, class... Ts, class F,
          class E = typename std::enable_if<is_map_fun<F>::value>::type>
stream_builder<In, result_type_t<F>, Ts..., F>
operator|(stream_builder<In, Out, Ts...> x, F f) {
  static_assert(std::is_same<Out, arg_type_t<F>>::value,
                "invalid map signature");
  return {std::tuple_cat(std::move(x.fs), std::make_tuple(f))};
}

template <class In, class Out, class... Ts, class F,
          class E = typename std::enable_if<is_reduce_fun<F>::value>::type>
result<typename std::decay<first_arg_type_t<F>>::type>
operator>>(stream_builder<In, Out, Ts...>, F) {
  return delegated<typename std::decay<first_arg_type_t<F>>::type>{};
}

template <class In, class Out, class... Fs>
stage_ptr<In, Out> make_stage(source_ptr<In> prev,
                              stream_builder<In, Out, Fs...>&& sb) {
  class impl : public stage<In, Out> {
  public:
    using super = stage<In, Out>;
    using tuple_type = std::tuple<Fs...>;
    using prev_stage = source_ptr<In>;

    impl(prev_stage ptr, tuple_type&& fs)
        : super(std::move(ptr)),
          fs_(std::move(fs)) {
      // nop
    }

    optional<Out> pull() {
      using namespace detail;
      auto x = this->prev().pull();
      eval_helper<In, Out> h{x};
      return apply_args(h, typename il_range<0, sizeof...(Fs)>::type{}, fs_);
    }

  private:
    tuple_type fs_;
  };
  return make_counted<impl>(std::move(prev), std::move(sb.fs));
}

std::pair<std::string, int> flatten(std::map<std::string, int>) {
  return std::make_pair("", 0);
}

} // namespace streaming

class ipair_pipe : public streaming::origin<std::pair<int, int>> {
public:
  using value_type = std::pair<int, int>;

  optional<value_type> pull() {
    if (xs_.empty())
      return none;
    auto x = xs_.front();
    xs_.erase(xs_.begin());
    return x;
  }

  void push(value_type x) {
    xs_.emplace_back(x);
  }

private:
  std::vector<value_type> xs_;
};

class stage1 : public streaming::stage<std::pair<int, int>, int> {
public:
  using super = streaming::stage<std::pair<int, int>, int>;

  using super::super;

  optional<int> pull() {
    while (xs_.empty()) {
      auto new_x = prev().pull();
      if (new_x == none)
        return none;
      for (int i = 0; i < new_x->first; ++i)
        xs_.push_back(new_x->second);
    }
    auto x = xs_.front();
    xs_.erase(xs_.begin());
    return x;
  }

private:
  std::vector<int> xs_;
};

class test_consumer : public streaming::consumer<int> {
public:
  void consume(std::vector<int>& buf) override {
    cout << "test_consumer: " << deep_to_string(buf) << endl;
  }

  void shutdown() override {
    cout << "test_consumer::shutdown" << endl;
  }
};

template <class T>
struct stream {
public:
  template <class In, class... Fs>
  stream(streaming::stream_builder<In, T, Fs...>) {
    // nop
  }
};

template <class T>
struct is_stream : std::false_type {};

template <class T>
struct is_stream<stream<T>> : std::true_type {};

} // namespace <anonymous>

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(stream<void>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(stream<int>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(stream<std::string>)

namespace {

template <class T>
stream<T> to_stream(T) {
  return {};
}

template <class T>
struct strip_optional;

template <class T>
struct strip_optional<optional<T>> {
  using type = T;
};

template <class T>
using strip_optional_t = typename strip_optional<T>::type;

template <class T>
streaming::stream_builder<T, T> transform(stream<T>&&) {
  return {};
}

streaming::stream_builder<std::string, std::string>
file_source(const std::string& path) {
  return {};
}

template <class T, class Destination>
struct stream_result {
  // t: trait representing the response type of the Destination for e
  using t = response_type<typename Destination::signatures, stream<T>>;
  static_assert(t::valid, "Destination does not accept streams of that type");
  // result type
  using type = typename t::type;
  static_assert(detail::tl_size<type>::value == 1,
                "Destination does not terminate the stream");
  static_assert(!is_stream<typename detail::tl_head<type>::type>::value,
                "Destination does not terminate the stream");
};

template <class Generator, class Destination>
using stream_result_t = typename stream_result<Generator, Destination>::type;

class event_based_actor_ : public event_based_actor {
public:
  event_based_actor_(actor_config& cfg) : event_based_actor(cfg) {
    // nop
  }

  template <class Dest, class Generator>
  response_handle<event_based_actor_, stream_result_t<strip_optional_t<streaming::result_type_t<Generator>>, Dest>, false>
  stream(Dest, Generator) {
    return {message_id::make(), this};
  }
};

// maps have signature A -> B
// filters have signature A -> bool
// terminal ops have signature A -> void

using foo1_actor = typed_actor<replies_to<stream<int>>::with<stream<std::string>>>;

foo1_actor::behavior_type foo1(foo1_actor::pointer self) {
  return {
    // stream handshake
    [=](stream<int>& source) -> stream<std::string> {
      return transform(std::move(source))
        | [](int x) { return x * 2.0; }
        | [](double x) { return x > 5.; }
        | [](double x) { return std::to_string(x); }
        ;
    }
  };
}

behavior foo2(event_based_actor_* self) {
  return {
    [=](std::string& x) {
      aout(self) << x << endl;
    }
  };
}

using path = std::string;
using wc_pair = std::pair<std::string, int>;
using mapped_stream = stream<wc_pair>;
using result_map = std::map<std::string, int>;

using file_mapper = typed_actor<replies_to<path>::with<mapped_stream>>;
using reducer = typed_actor<replies_to<mapped_stream>::with<result_map>>;

reducer::behavior_type reducer_impl() {
  return {
    [](mapped_stream& x) -> result<result_map> {
      return transform(std::move(x))
        >> [](result_map& res, wc_pair x) {
             auto i = res.find(x.first);
             if (i == res.end())
               res.emplace(std::move(x.first), x.second);
             else
               i->second += x.second;
           };
    }
  };
}

file_mapper::behavior_type file_mapper_impl() {
  return {
    [](path& p) -> mapped_stream {
      return file_source(p)
        | [](std::string line) -> std::map<std::string, int> {
            std::map<std::string, int> result;
            std::vector<std::string> lines;
            split(lines, line, " ", token_compress_on);
            for (auto& x : lines)
              ++result[x];
            return result;
          }
        | streaming::flatten;
    }
  };
}

void test(actor_system& sys) {
  auto f = sys.spawn(file_mapper_impl);
  auto g = sys.spawn(reducer_impl);
  auto pipeline = g * f;
  scoped_actor self{sys};
  self->request(pipeline, infinite, "words.txt").receive(
    [&](result_map& result) {
      aout(self) << "result = " << deep_to_string(result) << endl;
    },
    [&](error& err) {
      aout(self) << "error: " << sys.render(err) << endl;
    }
  );
}

using foo3_actor = typed_actor<replies_to<stream<int>>::with<void>>;

foo3_actor::behavior_type foo3(foo3_actor::pointer self) {
  return {
    // stream handshake
    [=](stream<int>& source) {
      transform(std::move(source))
        | [](int x) { return x * 2.0; }
        | [](double x) { return x > 5.; }
        | [](double x) { return std::to_string(x); }
        | [=](std::string x) { aout(self) << x << endl; }
        ;
    }
  };
}

template <class T>
class generator : public ref_counted {
public:
  /**
   * Returns how many credit is currently available.
   */
  size_t credit() const {
    return credit_;
  }

  /**
   * Grant new credit for sending messages.
   */
  size_t grant_credit(size_t x) {
    credit_ += x;
  }

  /**
   * Consumes up to `n` elements using the function object `f`.
   * @returns the number of consumed elements
   */
  virtual size_t consume(std::function<void (T&)> f, size_t num) = 0;

  /**
   * Returns whether this generator is closed.
   */
  bool closed() const {
    return closed_;
  }

  /**
   * Close this generator, i.e., transmit remaining items
   * and shutdown the stream aferwards.
   */
  void close() const {
    closed_ = true;
  }

private:
  size_t credit_;
  bool closed_;
};

/**
 * Represents a queue for outgoing data of type T.
 */
template <class T>
class output_queue : public generator<T> {
public:
  /**
   * Adds a new element to the queue.
   */
  void push(T x) {
    xs_.emplace_back(std::move(x));
  }

  /**
   * Adds `(first, last]` to the queue.
   */
  template <class Iter>
  void push(Iter first, Iter last) {
    xs_.insert(xs_.end(), first, last);
  }

  /**
   * Adds `xs` to the queue.
   */
  void push(std::initializer_list<T> xs) {
    push(xs.begin(), xs.end());
  }

  size_t size() const {
    return xs_.size();
  }

  size_t consume(std::function<void (T&)> f, size_t num) override {
    auto n = std::min({num, this->credit(), xs_.size()});
    auto first = xs_.begin();
    auto last = first + n;
    std::for_each(first, last, f);
    xs_.erase(first, last);
    return n;
  }

private:
  std::vector<T> xs_;
};

template <class T>
using output_queue_ptr = intrusive_ptr<output_queue<T>>;

template <class Container>
struct finite_generator {
public:
  using value_type = typename Container::value_type;
  using iterator = typename Container::iterator;

  finite_generator(Container&& xs) : xs_(std::move(xs)) {
    pos_ = xs_.begin();
    end_ = xs_.end();
  }

  optional<value_type> operator()() {
    if (pos_ == end_)
      return none;
    return std::move(*pos_++);
  }

private:
  Container xs_;
  iterator pos_;
  iterator end_;
};

enum some_error : uint8_t {
  queue_full = 1
};

error make_error(some_error x) {
  return {static_cast<uint8_t>(x), atom("some-error")};
}

constexpr int threshold = 10;

behavior edge_actor(event_based_actor* self, output_queue_ptr<int> q) {
  return {
    [=](int task) -> result<void> {
      // drop message if no 
      if (q->size() >= threshold) {
        return some_error::queue_full;
      q->push(task);
      return unit;
      }
    }
  };
}

template <class Container>
finite_generator<Container> make_generator(Container xs) {
  return {std::move(xs)};
}

void bar(event_based_actor_* self) {
  static_assert(is_stream<stream<std::string>>::value, "...");
  std::vector<int> xs{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto f1 = self->spawn(foo1);
  //auto composed = self->spawn(foo2) * f1;
  auto single = self->spawn(foo3);
  self->stream(single, make_generator(xs)).then(
    [=] {
      aout(self) << "stream is done" << endl;
    }
  );
}

void caf_main(actor_system& system, const config&) {

  system.spawn(bar);
  system.await_all_actors_done();

  using std::make_pair;
  auto v0 = make_counted<ipair_pipe>();
  auto v1 = make_counted<stage1>(v0);
  auto l = make_counted<test_consumer>();
  auto ch = make_counted<streaming::pipeline<std::pair<int, int>, int>>(v0, v1, l);

  cout << "push without providing credit first" << endl;
  ch->push({make_pair(2, 10), make_pair(0, 10), make_pair(10, 0)});
  cout << "provide 12 credit (read all)" << endl;
  ch->grant_credit(12);
  cout << "provide 20 more credit" << endl;
  ch->grant_credit(20);
  cout << "push 5 elements" << endl;
  ch->push({make_pair(5, 50)});
  cout << "push 15 elements" << endl;
  ch->push({make_pair(15, 10)});
  cout << "push 20 elements" << endl;
  ch->push({make_pair(10, 1), make_pair(10, 2)});
  cout << "grant 5 credit" << endl;
  ch->grant_credit(5);
  cout << "close stream" << endl;
  ch->close();
  cout << "grant 5 credit" << endl;
  ch->grant_credit(5);
  cout << "grant 5 credit" << endl;
  ch->grant_credit(5);
  cout << "grant 6 credit" << endl;
  ch->grant_credit(6);

  streaming::stream_builder<int, int> sb;

  auto x = sb | [](int x) { return x * 2.0; }
              | [](double x) { return x < 1.; }
              | [](double x) { return std::to_string(x); };

  cout << "x(10) = " << eval(x, 10) << endl;

  cout << "x(10) = " << eval(x, 0) << endl;

  auto y = sb | [](int x) { return x % 2 == 1; };

  cout << "y(3) = " << eval(y, 3) << endl;
  cout << "y(4) = " << eval(y, 4) << endl;

  /*
  scoped_actor self{system};
  actor dest = self->spawn(sink) * self->spawn(forward_stage);
  printf("expect %d number of inputs at sink\n",
         (int) cfg.num_sources * cfg.num_msgs);
  for (size_t i = 0; i < cfg.num_sources; ++i)
    self->spawn(source, dest, cfg.num_msgs);
  */
}

} // namespace <anonymous>

CAF_MAIN()
