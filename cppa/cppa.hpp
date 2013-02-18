/******************************************************************************\
 *           ___        __                                                    *
 *          /\_ \    __/\ \                                                   *
 *          \//\ \  /\_\ \ \____    ___   _____   _____      __               *
 *            \ \ \ \/\ \ \ '__`\  /'___\/\ '__`\/\ '__`\  /'__`\             *
 *             \_\ \_\ \ \ \ \L\ \/\ \__/\ \ \L\ \ \ \L\ \/\ \L\.\_           *
 *             /\____\\ \_\ \_,__/\ \____\\ \ ,__/\ \ ,__/\ \__/.\_\          *
 *             \/____/ \/_/\/___/  \/____/ \ \ \/  \ \ \/  \/__/\/_/          *
 *                                          \ \_\   \ \_\                     *
 *                                           \/_/    \/_/                     *
 *                                                                            *
 * Copyright (C) 2011-2013                                                    *
 * Dominik Charousset <dominik.charousset@haw-hamburg.de>                     *
 *                                                                            *
 * This file is part of libcppa.                                              *
 * libcppa is free software: you can redistribute it and/or modify it under   *
 * the terms of the GNU Lesser General Public License as published by the     *
 * Free Software Foundation, either version 3 of the License                  *
 * or (at your option) any later version.                                     *
 *                                                                            *
 * libcppa is distributed in the hope that it will be useful,                 *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of             *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.                       *
 * See the GNU Lesser General Public License for more details.                *
 *                                                                            *
 * You should have received a copy of the GNU Lesser General Public License   *
 * along with libcppa. If not, see <http://www.gnu.org/licenses/>.            *
\******************************************************************************/


#ifndef CPPA_HPP
#define CPPA_HPP

#include <tuple>
#include <chrono>
#include <cstdint>
#include <functional>
#include <type_traits>

#include "cppa/on.hpp"
#include "cppa/atom.hpp"
#include "cppa/self.hpp"
#include "cppa/actor.hpp"
#include "cppa/match.hpp"
#include "cppa/channel.hpp"
#include "cppa/receive.hpp"
#include "cppa/factory.hpp"
#include "cppa/behavior.hpp"
#include "cppa/announce.hpp"
#include "cppa/sb_actor.hpp"
#include "cppa/scheduler.hpp"
#include "cppa/to_string.hpp"
#include "cppa/any_tuple.hpp"
#include "cppa/cow_tuple.hpp"
#include "cppa/tuple_cast.hpp"
#include "cppa/exit_reason.hpp"
#include "cppa/local_actor.hpp"
#include "cppa/message_future.hpp"
#include "cppa/response_handle.hpp"
#include "cppa/scheduled_actor.hpp"
#include "cppa/scheduling_hint.hpp"
#include "cppa/event_based_actor.hpp"

#include "cppa/util/rm_ref.hpp"
#include "cppa/network/acceptor.hpp"

#include "cppa/detail/memory.hpp"
#include "cppa/detail/actor_count.hpp"
#include "cppa/detail/get_behavior.hpp"
#include "cppa/detail/receive_loop_helper.hpp"

/**
 * @author Dominik Charousset <dominik.charousset (at) haw-hamburg.de>
 *
 * @mainpage libcppa
 *
 * @section Intro Introduction
 *
 * This library provides an implementation of the actor model for C++.
 * It uses a network transparent messaging system to ease development
 * of both concurrent and distributed software.
 *
 * @p libcppa uses a thread pool to schedule actors by default.
 * A scheduled actor should not call blocking functions.
 * Individual actors can be spawned (created) with a special flag to run in
 * an own thread if one needs to make use of blocking APIs.
 *
 * Writing applications in @p libcppa requires a minimum of gluecode and
 * each context <i>is</i> an actor. Even main is implicitly
 * converted to an actor if needed.
 *
 * @section GettingStarted Getting Started
 *
 * To build @p libcppa, you need <tt>GCC >= 4.7</tt> or <tt>Clang >= 3.2</tt>,
 * and @p CMake.
 *
 * The usual build steps on Linux and Mac OS X are:
 *
 *- <tt>mkdir build</tt>
 *- <tt>cd build</tt>
 *- <tt>cmake ..</tt>
 *- <tt>make</tt>
 *- <tt>make install</tt> (as root, optionally)
 *
 * Please run the unit tests as well to verify that @p libcppa works properly.
 *
 *- <tt>./bin/unit_tests</tt>
 *
 * Please submit a bug report that includes (a) your compiler version,
 * (b) your OS, and (c) the output of the unit tests if an error occurs.
 *
 * Windows is not supported yet, because MVSC++ doesn't implement the
 * C++11 features needed to compile @p libcppa.
 *
 * Please read the <b>Manual</b> for an introduction to @p libcppa.
 * It is available online at
 * http://neverlord.github.com/libcppa/manual/index.html or as PDF version at
 * http://neverlord.github.com/libcppa/manual/libcppa_manual.pdf
 *
 * @section IntroHelloWorld Hello World Example
 *
 * @include hello_world_example.cpp
 *
 * @section IntroMoreExamples More Examples
 *
 * The {@link math_actor_example.cpp Math Actor Example} shows the usage
 * of {@link receive_loop} and {@link cppa::arg_match arg_match}.
 * The {@link dining_philosophers.cpp Dining Philosophers Example}
 * introduces event-based actors and includes a lot of <tt>libcppa</tt>
 * features.
 *
 * @namespace cppa
 * @brief Root namespace of libcppa.
 *
 * @namespace cppa::util
 * @brief Contains utility classes and metaprogramming
 *        utilities used by the libcppa implementation.
 *
 * @namespace cppa::intrusive
 * @brief Contains intrusive container implementations.
 *
 * @namespace cppa::network
 * @brief Contains all network related classes.
 *
 * @namespace cppa::factory
 * @brief Contains factory functions to create actors from lambdas or
 *        other functors.
 *
 * @namespace cppa::exit_reason
 * @brief Contains all predefined exit reasons.
 *
 * @namespace cppa::placeholders
 * @brief Contains the guard placeholders @p _x1 to @p _x9.
 *
 * @defgroup CopyOnWrite Copy-on-write optimization.
 * @p libcppa uses a copy-on-write optimization for its message
 * passing implementation.
 *
 * {@link cppa::cow_tuple Tuples} should @b always be used with by-value
 * semantic,since tuples use a copy-on-write smart pointer internally.
 * Let's assume two
 * tuple @p x and @p y, whereas @p y is a copy of @p x:
 *
 * @code
 * auto x = make_cow_tuple(1, 2, 3);
 * auto y = x;
 * @endcode
 *
 * Those two tuples initially point to the same data (the addresses of the
 * first element of @p x is equal to the address of the first element
 * of @p y):
 *
 * @code
 * assert(&(get<0>(x)) == &(get<0>(y)));
 * @endcode
 *
 * <tt>get<0>(x)</tt> returns a const-reference to the first element of @p x.
 * The function @p get does not have a const-overload to avoid
 * unintended copies. The function @p get_ref could be used to
 * modify tuple elements. A call to this function detaches
 * the tuple by copying the data before modifying it if there are two or more
 * references to the data:
 *
 * @code
 * // detaches x from y
 * get_ref<0>(x) = 42;
 * // x and y no longer point to the same data
 * assert(&(get<0>(x)) != &(get<0>(y)));
 * @endcode
 *
 * @defgroup MessageHandling Message handling.
 *
 * This is the beating heart of @p libcppa. Actor programming is all about
 * message handling.
 *
 * A message in @p libcppa is a n-tuple of values (with size >= 1). You can use
 * almost every type in a messages.
 *
 * @section Send Send messages
 *
 * The function @p send could be used to send a message to an actor.
 * The first argument is the receiver of the message followed by any number
 * of values. @p send creates a tuple from the given values and enqueues the
 * tuple to the receivers mailbox. Thus, send should @b not be used to send
 * a message to multiple receivers. You should use @p operator<<
 * instead as in the following example:
 *
 * @code
 * // spawn some actors
 * auto a1 = spawn(...);
 * auto a2 = spawn(...);
 * auto a3 = spawn(...);
 *
 * // send a message to a1
 * send(a1, atom("hello"), "hello a1!");
 *
 * // send a message to a1, a2 and a3
 * auto msg = make_cow_tuple(atom("compute"), 1, 2, 3);
 *
 * // note: this is more efficient then using send() three times because
 * //       send() would create a new tuple each time;
 * //       this safes both time and memory thanks to libcppa's copy-on-write
 * a1 << msg;
 * a2 << msg;
 * a3 << msg;
 *
 * // modify msg and send it again
 * // (msg becomes detached due to copy-on-write optimization)
 * get_ref<1>(msg) = 10; // msg is now { atom("compute"), 10, 2, 3 }
 * a1 << msg;
 * a2 << msg;
 * a3 << msg;
 * @endcode
 *
 * @section Receive Receive messages
 *
 * The function @p receive takes a @p behavior as argument. The behavior
 * is a list of { pattern >> callback } rules.
 *
 * @code
 * receive
 * (
 *     on(atom("hello"), arg_match) >> [](const std::string& msg)
 *     {
 *         cout << "received hello message: " << msg << endl;
 *     },
 *     on(atom("compute"), arg_match) >> [](int i0, int i1, int i2)
 *     {
 *         // send our result back to the sender of this messages
 *         reply(atom("result"), i0 + i1 + i2);
 *     }
 * );
 * @endcode
 *
 * Please read the manual for further details about pattern matching.
 *
 * @section Atoms Atoms
 *
 * Atoms are a nice way to add semantic informations to a message.
 * Assuming an actor wants to provide a "math sevice" for integers. It
 * could provide operations such as addition, subtraction, etc.
 * This operations all have two operands. Thus, the actor does not know
 * what operation the sender of a message wanted by receiving just two integers.
 *
 * Example actor:
 * @code
 * void math_actor() {
 *     receive_loop (
 *         on(atom("plus"), arg_match) >> [](int a, int b) {
 *             reply(atom("result"), a + b);
 *         },
 *         on(atom("minus"), arg_match) >> [](int a, int b) {
 *             reply(atom("result"), a - b);
 *         }
 *     );
 * }
 * @endcode
 *
 * @section ReceiveLoops Receive loops
 *
 * Previous examples using @p receive create behaviors on-the-fly.
 * This is inefficient in a loop since the argument passed to receive
 * is created in each iteration again. It's possible to store the behavior
 * in a variable and pass that variable to receive. This fixes the issue
 * of re-creation each iteration but rips apart definition and usage.
 *
 * There are four convenience functions implementing receive loops to
 * declare behavior where it belongs without unnecessary
 * copies: @p receive_loop, @p receive_while, @p receive_for and @p do_receive.
 *
 * @p receive_loop is analogous to @p receive and loops "forever" (until the
 * actor finishes execution).
 *
 * @p receive_while creates a functor evaluating a lambda expression.
 * The loop continues until the given lambda returns @p false. A simple example:
 *
 * @code
 * // receive two integers
 * vector<int> received_values;
 * receive_while([&]() { return received_values.size() < 2; }) (
 *     on<int>() >> [](int value) {
 *         received_values.push_back(value);
 *     }
 * );
 * // ...
 * @endcode
 *
 * @p receive_for is a simple ranged-based loop:
 *
 * @code
 * std::vector<int> vec {1, 2, 3, 4};
 * auto i = vec.begin();
 * receive_for(i, vec.end()) (
 *     on(atom("get")) >> [&]() { reply(atom("result"), *i); }
 * );
 * @endcode
 *
 * @p do_receive returns a functor providing the function @p until that
 * takes a lambda expression. The loop continues until the given lambda
 * returns true. Example:
 *
 * @code
 * // receive ints until zero was received
 * vector<int> received_values;
 * do_receive (
 *     on<int>() >> [](int value) {
 *         received_values.push_back(value);
 *     }
 * )
 * .until([&]() { return received_values.back() == 0 });
 * // ...
 * @endcode
 *
 * @section FutureSend Send delayed messages
 *
 * The function @p delayed_send provides a simple way to delay a message.
 * This is particularly useful for recurring events, e.g., periodical polling.
 * Usage example:
 *
 * @code
 * delayed_send(self, std::chrono::seconds(1), atom("poll"));
 * receive_loop (
 *     // ...
 *     on(atom("poll")) >> []() {
 *         // ... poll something ...
 *         // and do it again after 1sec
 *         delayed_send(self, std::chrono::seconds(1), atom("poll"));
 *     }
 * );
 * @endcode
 *
 * See also the {@link dancing_kirby.cpp dancing kirby example}.
 *
 * @defgroup ImplicitConversion Implicit type conversions.
 *
 * The message passing of @p libcppa prohibits pointers in messages because
 * it enforces network transparent messaging.
 * Unfortunately, string literals in @p C++ have the type <tt>const char*</tt>,
 * resp. <tt>const char[]</tt>. Since @p libcppa is a user-friendly library,
 * it silently converts string literals and C-strings to @p std::string objects.
 * It also converts unicode literals to the corresponding STL container.
 *
 * A few examples:
 * @code
 * // sends an std::string containing "hello actor!" to itself
 * send(self, "hello actor!");
 *
 * const char* cstring = "cstring";
 * // sends an std::string containing "cstring" to itself
 * send(self, cstring);
 *
 * // sends an std::u16string containing the UTF16 string "hello unicode world!"
 * send(self, u"hello unicode world!");
 *
 * // x has the type cppa::tuple<std::string, std::string>
 * auto x = make_cow_tuple("hello", "tuple");
 *
 * receive (
 *     // equal to: on(std::string("hello actor!"))
 *     on("hello actor!") >> []() { }
 * );
 * @endcode
 *
 * @defgroup ActorCreation Actor creation.
 *
 */

// examples

/**
 * @brief A trivial example program.
 * @example hello_world_example.cpp
 */

/**
 * @brief Shows the usage of {@link cppa::atom atoms}
 *        and {@link cppa::arg_match arg_match}.
 * @example math_actor_example.cpp
 */

/**
 * @brief A simple example for a delayed_send based application.
 * @example dancing_kirby.cpp
 */

/**
 * @brief An event-based "Dining Philosophers" implementation.
 * @example dining_philosophers.cpp
 */

namespace cppa {

namespace detail {

template<typename T>
inline void send_impl(T* whom, any_tuple&& what) {
    if (whom) self->send_message(whom, std::move(what));
}

template<typename T, typename... Args>
inline void send_tpl_impl(T* whom, Args&&... what) {
    static_assert(sizeof...(Args) > 0, "no message to send");
    if (whom) self->send_message(whom,
                                 make_any_tuple(std::forward<Args>(what)...));
}

} // namespace detail

/**
 * @ingroup MessageHandling
 * @{
 */

/**
 * @brief Sends @p what as a message to @p whom.
 * @param whom Receiver of the message.
 * @param what Message content as tuple.
 */
template<class C, typename... Args>
inline typename std::enable_if<std::is_base_of<channel, C>::value>::type
send_tuple(const intrusive_ptr<C>& whom, any_tuple what) {
    detail::send_impl(whom.get(), std::move(what));
}

/**
 * @brief Sends <tt>{what...}</tt> as a message to @p whom.
 * @param whom Receiver of the message.
 * @param what Message elements.
 * @pre <tt>sizeof...(Args) > 0</tt>
 */
template<class C, typename... Args>
inline typename std::enable_if<std::is_base_of<channel, C>::value>::type
send(const intrusive_ptr<C>& whom, Args&&... what) {
    detail::send_tpl_impl(whom.get(), std::forward<Args>(what)...);
}

/**
 * @brief Sends @p what as a message to @p whom, but sets
 *        the sender information to @p from.
 * @param from Sender as seen by @p whom.
 * @param whom Receiver of the message.
 * @param what Message elements.
 * @pre <tt>sizeof...(Args) > 0</tt>
 */
template<class C, typename... Args>
inline typename std::enable_if<std::is_base_of<channel, C>::value>::type
send_tuple_as(const actor_ptr& from, const intrusive_ptr<C>& whom, any_tuple what) {
    if (whom) whom->enqueue(from.get(), std::move(what));
}

/**
 * @brief Sends <tt>{what...}</tt> as a message to @p whom, but sets
 *        the sender information to @p from.
 * @param from Sender as seen by @p whom.
 * @param whom Receiver of the message.
 * @param what Message elements.
 * @pre <tt>sizeof...(Args) > 0</tt>
 */
template<class C, typename... Args>
inline typename std::enable_if<std::is_base_of<channel, C>::value>::type
send_as(const actor_ptr& from, const intrusive_ptr<C>& whom, Args&&... what) {
    send_tuple_as(from, whom, make_any_tuple(std::forward<Args>(what)...));
}

/**
 * @brief Sends a message to @p whom.
 *
 * <b>Usage example:</b>
 * @code
 * self << make_any_tuple(1, 2, 3);
 * @endcode
 * @param whom Receiver of the message.
 * @param what Message as instance of {@link any_tuple}.
 * @returns @p whom.
 */
template<class C>
inline typename std::enable_if<std::is_base_of<channel, C>::value,
                               const intrusive_ptr<C>&            >::type
operator<<(const intrusive_ptr<C>& whom, any_tuple what) {
    send_tuple(whom, std::move(what));
    return whom;
}

/**
 * @brief Sends @p what as a synchronous message to @p whom.
 * @param whom Receiver of the message.
 * @param what Message content as tuple.
 * @returns A handle identifying a future to the response of @p whom.
 * @warning The returned handle is actor specific and the response to the sent
 *          message cannot be received by another actor.
 * @throws std::invalid_argument if <tt>whom == nullptr</tt>
 */
inline message_future sync_send_tuple(const actor_ptr& whom, any_tuple what) {
    if (whom) return self->send_sync_message(whom.get(), std::move(what));
    else throw std::invalid_argument("whom == nullptr");
}

/**
 * @brief Sends <tt>{what...}</tt> as a synchronous message to @p whom.
 * @param whom Receiver of the message.
 * @param what Message elements.
 * @returns A handle identifying a future to the response of @p whom.
 * @warning The returned handle is actor specific and the response to the sent
 *          message cannot be received by another actor.
 * @pre <tt>sizeof...(Args) > 0</tt>
 * @throws std::invalid_argument if <tt>whom == nullptr</tt>
 */
template<typename... Args>
inline message_future sync_send(const actor_ptr& whom, Args&&... what) {
    static_assert(sizeof...(Args) > 0, "no message to send");
    return sync_send_tuple(whom, make_any_tuple(std::forward<Args>(what)...));
}

/**
 * @brief Sends @p what as a synchronous message to @p whom with a timeout.
 *
 * The calling actor receives a 'TIMEOUT' message as response after
 * given timeout exceeded and no response messages was received.
 * @param whom Receiver of the message.
 * @param what Message content as tuple.
 * @returns A handle identifying a future to the response of @p whom.
 * @warning The returned handle is actor specific and the response to the sent
 *          message cannot be received by another actor.
 * @throws std::invalid_argument if <tt>whom == nullptr</tt>
 */
template<class Rep, class Period, typename... Args>
message_future timed_sync_send_tuple(const actor_ptr& whom,
                                     const std::chrono::duration<Rep,Period>& rel_time,
                                     any_tuple what) {
    if (whom) return self->send_timed_sync_message(whom.get(),
                                                   rel_time,
                                                   std::move(what));
    else throw std::invalid_argument("whom == nullptr");
}

/**
 * @brief Sends <tt>{what...}</tt> as a synchronous message to @p whom
 *        with a timeout.
 *
 * The calling actor receives a 'TIMEOUT' message as response after
 * given timeout exceeded and no response messages was received.
 * @param whom Receiver of the message.
 * @param what Message elements.
 * @returns A handle identifying a future to the response of @p whom.
 * @warning The returned handle is actor specific and the response to the sent
 *          message cannot be received by another actor.
 * @pre <tt>sizeof...(Args) > 0</tt>
 * @throws std::invalid_argument if <tt>whom == nullptr</tt>
 */
template<class Rep, class Period, typename... Args>
message_future timed_sync_send(const actor_ptr& whom,
                               const std::chrono::duration<Rep,Period>& rel_time,
                               Args&&... what) {
    static_assert(sizeof...(Args) > 0, "no message to send");
    return timed_sync_send_tuple(whom,
                                 rel_time,
                                 make_any_tuple(std::forward<Args>(what)...));
}

/**
 * @brief Handles a synchronous response message in an event-based way.
 * @param handle A future for a synchronous response.
 * @throws std::invalid_argument if given behavior does not has a valid
 *                               timeout definition
 * @throws std::logic_error if @p handle is not valid or if the actor
 *                          already received the response for @p handle
 */
sync_recv_helper receive_response(const message_future& handle);

/**
 * @brief Sends a message to the sender of the last received message.
 * @param what Message content as a tuple.
 */
inline void reply_tuple(any_tuple what) {
    self->reply_message(std::move(what));
}

/**
 * @brief Sends a message to the sender of the last received message.
 * @param what Message elements.
 */
template<typename... Args>
inline void reply(Args&&... what) {
    self->reply_message(make_any_tuple(std::forward<Args>(what)...));
}

/**
 * @brief Sends a message as reply to @p handle.
 */
template<typename... Args>
inline void reply_to(const response_handle& handle, Args&&... what) {
    if (handle.valid()) {
        handle.apply(make_any_tuple(std::forward<Args>(what)...));
    }
}

/**
 * @brief Sends a message to the sender of the last received message.
 * @param what Message content as a tuple.
 */
inline void reply_tuple_to(const response_handle& handle, any_tuple what) {
    handle.apply(std::move(what));
}

/**
 * @brief Forwards the last received message to @p whom.
 */
inline void forward_to(const actor_ptr& whom) {
    self->forward_message(whom);
}

/**
 * @brief Sends a message to @p whom that is delayed by @p rel_time.
 * @param whom Receiver of the message.
 * @param rtime Relative time duration to delay the message in
 *              microseconds, milliseconds, seconds or minutes.
 * @param what Message content as a tuple.
 */
template<class Rep, class Period, typename... Args>
inline void delayed_send_tuple(const channel_ptr& whom,
                               const std::chrono::duration<Rep,Period>& rtime,
                               any_tuple what) {
    if (whom) get_scheduler()->delayed_send(whom, rtime, what);
}

/**
 * @brief Sends a message to @p whom that is delayed by @p rel_time.
 * @param whom Receiver of the message.
 * @param rtime Relative time duration to delay the message in
 *              microseconds, milliseconds, seconds or minutes.
 * @param what Message elements.
 */
template<class Rep, class Period, typename... Args>
inline void delayed_send(const channel_ptr& whom,
                         const std::chrono::duration<Rep,Period>& rtime,
                         Args&&... what) {
    static_assert(sizeof...(Args) > 0, "no message to send");
    if (whom) {
        delayed_send_tuple(whom,
                           rtime,
                           make_any_tuple(std::forward<Args>(what)...));
    }
}

/**
 * @brief Sends a reply message that is delayed by @p rel_time.
 * @param rtime Relative time duration to delay the message in
 *              microseconds, milliseconds, seconds or minutes.
 * @param what Message content as a tuple.
 * @see delayed_send()
 */
template<class Rep, class Period, typename... Args>
inline void delayed_reply_tuple(const std::chrono::duration<Rep, Period>& rtime,
                                any_tuple what) {
    get_scheduler()->delayed_reply(self->last_sender(),
                                   rtime,
                                   self->get_response_id(),
                                   std::move(what));
}

/**
 * @brief Sends a reply message that is delayed by @p rel_time.
 * @param rtime Relative time duration to delay the message in
 *              microseconds, milliseconds, seconds or minutes.
 * @param what Message elements.
 * @see delayed_send()
 */
template<class Rep, class Period, typename... Args>
inline void delayed_reply(const std::chrono::duration<Rep, Period>& rtime,
                          Args&&... what) {
    delayed_reply_tuple(rtime, make_any_tuple(std::forward<Args>(what)...));
}

/** @} */

#ifndef CPPA_DOCUMENTATION
// matches "send(this, ...)" and "send(self, ...)"
inline void send_tuple(local_actor* whom, any_tuple what) {
    detail::send_impl(whom, std::move(what));
}
template<typename... Args>
inline void send(local_actor* whom, Args&&... args) {
    detail::send_tpl_impl(whom, std::forward<Args>(args)...);
}
inline const self_type& operator<<(const self_type& s, any_tuple what) {
    detail::send_impl(s.get(), std::move(what));
    return s;
}
#endif // CPPA_DOCUMENTATION

/**
 * @ingroup ActorCreation
 * @{
 */

/**
 * @brief Spawns a new context-switching or thread-mapped {@link actor}
 *        that executes <tt>fun(args...)</tt>.
 * @param fun A function implementing the actor's behavior.
 * @param args Optional function parameters for @p fun.
 * @tparam Hint A hint to the scheduler for the best scheduling strategy.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 */
template<scheduling_hint Hint, typename Fun, typename... Args>
actor_ptr spawn(Fun&& fun, Args&&... args) {
    return get_scheduler()->spawn_impl(Hint,
                                       std::forward<Fun>(fun),
                                       std::forward<Args>(args)...);
}

/**
 * @brief Spawns a new context-switching {@link actor}
 *        that executes <tt>fun(args...)</tt>.
 * @param fun A function implementing the actor's behavior.
 * @param args Optional function parameters for @p fun.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 * @note This function is equal to <tt>spawn<scheduled>(fun, args...)</tt>.
 */
template<typename Fun, typename... Args>
actor_ptr spawn(Fun&& fun, Args&&... args) {
    return spawn<scheduled>(std::forward<Fun>(fun),
                            std::forward<Args>(args)...);
}

/**
 * @brief Spawns a new context-switching or thread-mapped {@link actor}
 *        that executes <tt>fun(args...)</tt> and
 *        joins @p grp immediately.
 * @param fun A function implementing the actor's behavior.
 * @param args Optional function parameters for @p fun.
 * @tparam Hint A hint to the scheduler for the best scheduling strategy.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 * @note The spawned actor joins @p grp after its
 *       {@link local_actor::init() init} member function is called but
 *       before it is executed. Hence, the spawned actor already joined
 *       the group before this function returns.
 */
template<scheduling_hint Hint, typename Fun, typename... Args>
actor_ptr spawn_in_group(const group_ptr& grp, Fun&& fun, Args&&... args) {
    return get_scheduler()->spawn_cb_impl(Hint,
                                          [grp](local_actor* ptr) {
                                              ptr->join(grp);
                                          },
                                          std::forward<Fun>(fun),
                                          std::forward<Args>(args)...);
}

/**
 * @brief Spawns a new context-switching {@link actor}
 *        that executes <tt>fun(args...)</tt> and
 *        joins @p grp immediately.
 * @param fun A function implementing the actor's behavior.
 * @param args Optional function parameters for @p fun.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 * @note The spawned actor joins @p grp after its
 *       {@link local_actor::init() init} member function is called but
 *       before it is executed. Hence, the spawned actor already joined
 *       the group before this function returns.
 * @note This function is equal to
 *       <tt>spawn_in_group<scheduled>(fun, args...)</tt>.
 */
template<typename Fun, typename... Args>
actor_ptr spawn_in_group(Fun&& fun, Args&&... args) {
    return spawn_in_group<scheduled>(std::forward<Fun>(fun),
                                     std::forward<Args>(args)...);
}

/**
 * @brief Spawns an actor of type @p ActorImpl.
 * @param args Optional constructor arguments.
 * @tparam ActorImpl Subtype of {@link event_based_actor} or {@link sb_actor}.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 */
template<class ActorImpl, typename... Args>
actor_ptr spawn(Args&&... args) {
    auto ptr = detail::memory::create<ActorImpl>(std::forward<Args>(args)...);
    return get_scheduler()->spawn(ptr);
}

/**
 * @brief Spawns an actor of type @p ActorImpl that joins @p grp immediately.
 * @param grp The group that the newly created actor shall join.
 * @param args Optional constructor arguments.
 * @tparam ActorImpl Subtype of {@link event_based_actor} or {@link sb_actor}.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 * @note The spawned actor joins @p grp after its
 *       {@link local_actor::init() init} member function is called but
 *       before it is executed. Hence, the spawned actor already joined
 *       the group before this function returns.
 */
template<class ActorImpl, typename... Args>
actor_ptr spawn_in_group(const group_ptr& grp, Args&&... args) {
    auto ptr = detail::memory::create<ActorImpl>(std::forward<Args>(args)...);
    return get_scheduler()->spawn(ptr, [&](local_actor* p) { p->join(grp); });
}

#ifndef CPPA_DOCUMENTATION

template<class ActorImpl, typename... Args>
actor_ptr spawn_hidden_in_group(const group_ptr& grp, Args&&... args) {
    auto ptr = detail::memory::create<ActorImpl>(std::forward<Args>(args)...);
    return get_scheduler()->spawn(ptr, [&](local_actor* p) { p->join(grp); },
                                  scheduled_and_hidden);
}

template<class ActorImpl, typename... Args>
actor_ptr spawn_hidden(Args&&... args) {
    auto ptr = detail::memory::create<ActorImpl>(std::forward<Args>(args)...);
    return get_scheduler()->spawn(ptr, scheduled_and_hidden);
}

#endif

/**
 * @brief Spawns a new context-switching or thread-mapped {@link actor}
 *        that executes <tt>fun(args...)</tt> and creates a link between the
 *        calling actor and the new actor.
 * @param fun A function implementing the actor's behavior.
 * @param args Optional function parameters for @p fun.
 * @tparam Hint A hint to the scheduler for the best scheduling strategy.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 */
template<scheduling_hint Hint, typename Fun, typename... Args>
actor_ptr spawn_link(Fun&& fun, Args&&... args) {
    auto res = spawn<Hint>(std::forward<Fun>(fun), std::forward<Args>(args)...);
    self->link_to(res);
    return res;
}

/**
 * @brief Spawns a new context-switching {@link actor}
 *        that executes <tt>fun(args...)</tt> and creates a link between the
 *        calling actor and the new actor.
 * @param fun A function implementing the actor's behavior.
 * @param args Optional function parameters for @p fun.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 * @note This function is equal to <tt>spawn<scheduled>(fun, args...)</tt>.
 */
template<typename Fun, typename... Args>
actor_ptr spawn_link(Fun&& fun, Args&&... args) {
    auto res = spawn(std::forward<Fun>(fun), std::forward<Args>(args)...);
    self->link_to(res);
    return res;
}

/**
 * @brief Spawns an actor of type @p ActorImpl and creates a link between the
 *        calling actor and the new actor.
 * @param args Optional constructor arguments.
 * @tparam ActorImpl Subtype of {@link event_based_actor} or {@link sb_actor}.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 */
template<class ActorImpl, typename... Args>
actor_ptr spawn_link(Args&&... args) {
    auto res = spawn<ActorImpl>(std::forward<Args>(args)...);
    self->link_to(res);
    return res;
}

/**
 * @brief Spawns a new context-switching or thread-mapped {@link actor}
 *        that executes <tt>fun(args...)</tt> and adds a monitor to the
 *        new actor.
 * @param fun A function implementing the actor's behavior.
 * @param args Optional function parameters for @p fun.
 * @tparam Hint A hint to the scheduler for the best scheduling strategy.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 */
template<scheduling_hint Hint, typename Fun, typename... Args>
actor_ptr spawn_monitor(Fun&& fun, Args&&... args) {
    auto res = spawn<Hint>(std::forward<Fun>(fun), std::forward<Args>(args)...);
    self->monitor(res);
    return res;
}

/**
 * @brief Spawns a new context-switching {@link actor}
 *        that executes <tt>fun(args...)</tt> and adds a monitor to the
 *        new actor.
 * @param fun A function implementing the actor's behavior.
 * @param args Optional function parameters for @p fun.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 * @note This function is equal to <tt>spawn<scheduled>(fun, args...)</tt>.
 */
template<typename Fun, typename... Args>
actor_ptr spawn_monitor(Fun&& fun, Args&&... args) {
    auto res = spawn(std::forward<Fun>(fun), std::forward<Args>(args)...);
    self->monitor(res);
    return res;
}

/**
 * @brief Spawns an actor of type @p ActorImpl and adds a monitor to the
 *        new actor.
 * @param args Optional constructor arguments.
 * @tparam ActorImpl Subtype of {@link event_based_actor} or {@link sb_actor}.
 * @returns An {@link actor_ptr} to the spawned {@link actor}.
 */
template<class ActorImpl, typename... Args>
actor_ptr spawn_monitor(Args&&... args) {
    auto res = spawn<ActorImpl>(std::forward<Args>(args)...);
    self->monitor(res);
    return res;
}

/** @} */

/**
 * @brief Blocks execution of this actor until all
 *        other actors finished execution.
 * @warning This function will cause a deadlock if called from multiple actors.
 * @warning Do not call this function in cooperatively scheduled actors.
 */
inline void await_all_others_done() {
    detail::actor_count_wait_until((self.unchecked() == nullptr) ? 0 : 1);
}

/**
 * @brief Publishes @p whom at @p port.
 *
 * The connection is automatically closed if the lifetime of @p whom ends.
 * @param whom Actor that should be published at @p port.
 * @param port Unused TCP port.
 * @param addr The IP address to listen to, or @p INADDR_ANY if @p addr is
 *             @p nullptr.
 * @throws bind_failure
 */
void publish(actor_ptr whom, std::uint16_t port, const char* addr = nullptr);

/**
 * @brief Publishes @p whom using @p acceptor to handle incoming connections.
 *
 * The connection is automatically closed if the lifetime of @p whom ends.
 * @param whom Actor that should be published at @p port.
 * @param acceptor Network technology-specific acceptor implementation.
 */
void publish(actor_ptr whom, std::unique_ptr<network::acceptor> acceptor);

/**
 * @brief Establish a new connection to the actor at @p host on given @p port.
 * @param host Valid hostname or IP address.
 * @param port TCP port.
 * @returns An {@link actor_ptr} to the proxy instance
 *          representing a remote actor.
 */
actor_ptr remote_actor(const char* host, std::uint16_t port);

/**
 * @copydoc remote_actor(const char*,std::uint16_t)
 */
inline actor_ptr remote_actor(const std::string& host, std::uint16_t port) {
    return remote_actor(host.c_str(), port);
}

/**
 * @brief Establish a new connection to the actor via given @p connection.
 * @param connection A connection to another libcppa process described by a pair
 *                   of input and output stream.
 * @returns An {@link actor_ptr} to the proxy instance
 *          representing a remote actor.
 */
actor_ptr remote_actor(network::io_stream_ptr_pair connection);

/**
 * @brief Destroys all singletons, disconnects all peers and stops the
 *        scheduler. It is recommended to use this function as very last
 *        function call before leaving main(). Especially in programs
 *        using libcppa's networking infrastructure.
 */
void shutdown(); // note: implemented in singleton_manager.cpp

/**
 * @brief Causes @p whom to quit with @p reason.
 * @note Does nothing if <tt>reason == exit_reason::normal</tt>.
 */
inline void quit_actor(const actor_ptr& whom, std::uint32_t reason) {
    CPPA_REQUIRE(reason != exit_reason::normal);
    send(whom, atom("EXIT"), reason);
}

struct actor_ostream {

    typedef const actor_ostream& (*fun_type)(const actor_ostream&);

    constexpr actor_ostream() { }

    inline const actor_ostream& write(std::string arg) const {
        send(get_scheduler()->printer(), atom("add"), move(arg));
        return *this;
    }

    inline const actor_ostream& flush() const {
        send(get_scheduler()->printer(), atom("flush"));
        return *this;
    }

};

namespace { constexpr actor_ostream aout; }

inline const actor_ostream& operator<<(const actor_ostream& o, std::string arg) {
    return o.write(move(arg));
}

inline const actor_ostream& operator<<(const actor_ostream& o, const any_tuple& arg) {
    return o.write(cppa::to_string(arg));
}

template<typename T>
inline typename std::enable_if<   !std::is_convertible<T,std::string>::value
                               && !std::is_convertible<T,any_tuple>::value,
                               const actor_ostream&>::type
operator<<(const actor_ostream& o, T&& arg) {
    return o.write(std::to_string(std::forward<T>(arg)));
}

inline const actor_ostream& operator<<(const actor_ostream& o, actor_ostream::fun_type f) {
    return f(o);
}

} // namespace cppa

namespace std {
// allow actor_ptr to be used in hash maps
template<>
struct hash<cppa::actor_ptr> {
    inline size_t operator()(const cppa::actor_ptr& ptr) const {
        return (ptr) ? static_cast<size_t>(ptr->id()) : 0;
    }
};
// provide convenience overlaods for aout; implemented in logging.cpp
const cppa::actor_ostream& endl(const cppa::actor_ostream& o);
const cppa::actor_ostream& flush(const cppa::actor_ostream& o);
} // namespace std

#endif // CPPA_HPP
