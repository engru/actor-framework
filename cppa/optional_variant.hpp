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
 * Free Software Foundation; either version 2.1 of the License,               *
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


#ifndef OPTIONAL_VARIANT_HPP
#define OPTIONAL_VARIANT_HPP

#include <stdexcept>
#include <type_traits>

#include "cppa/none.hpp"

#include "cppa/util/type_list.hpp"
#include "cppa/util/void_type.hpp"
#include "cppa/util/type_traits.hpp"

#include "cppa/detail/optional_variant_data.hpp"

#define CPPA_OPTIONAL_VARIANT_CASE(x)                                          \
    case x: return do_visit(from, visitor,                                     \
                            make_bool_token<void_pos == x >(),                 \
                            make_int_token< x >())                             \

namespace cppa {

template<int Value>
constexpr std::integral_constant<int, Value> make_int_token() { return {}; }

template<bool Value>
constexpr std::integral_constant<bool, Value> make_bool_token() { return {}; }

/**
 * @brief A optional_variant is either invalid or holds
          a value of one of the types <tt>Ts</tt>.
 */
template<typename... Ts>
class optional_variant {

 public:

    typedef util::type_list<Ts...> types;

    static constexpr int void_pos = util::tl_find<types, void>::value;

    /**
     * @brief Checks whether this objects holds a value of type @p T.
     */
    template<typename T>
    inline bool is() const {
        return m_type != -1 && m_type == util::tl_find<types, T>::value;
    }

    template<typename U>
    optional_variant& operator=(U&& arg) {
        destroy_data();
        set(std::forward<U>(arg));
        return *this;
    }

    optional_variant() : m_type(-1) { }

    template<typename U>
    optional_variant(U&& arg) {
        set(std::forward<U>(arg));
    }

    ~optional_variant() {
        destroy_data();
    }

    /**
     * @brief Checks whether this optional_variant is valid.
     */
    inline explicit operator bool() const {
        return m_type != -1;
    }

    /**
     * @brief Checks whether this optional_variant is invalid.
     */
    inline bool operator!() const {
        return m_type == -1;
    }

    /** @cond PRIVATE */

    template<int Pos>
    inline const typename util::tl_at<types, Pos>::type&
    get(std::integral_constant<int, Pos> token,
        typename std::enable_if<Pos < sizeof...(Ts) && Pos != void_pos>::type* = nullptr) const {
        return m_data.get(token);
    }

    template<int Pos>
    inline void get(std::integral_constant<int, Pos>,
                    typename std::enable_if<Pos >= sizeof...(Ts) || Pos == void_pos>::type* = nullptr) const { }

    template<int Pos>
    inline typename util::tl_at<types, Pos>::type&
    get(std::integral_constant<int, Pos> token,
        typename std::enable_if<Pos < sizeof...(Ts) && Pos != void_pos>::type* = nullptr) {
        return m_data.get(token);
    }

    template<typename Visitor>
    auto apply(const Visitor& visitor) const -> decltype(visitor(none_t{})) {
        return do_apply(*this, visitor);
    }

    template<typename Visitor>
    auto apply(const Visitor& visitor) -> decltype(visitor(none_t{})) {
        return do_apply(*this, visitor);
    }

    /** @endcond */

 private:

    template<typename Self, typename Visitor, int Pos>
    static auto do_visit(Self& from,
                         const Visitor& visitor,
                         std::integral_constant<bool, false> /* is_not_void */,
                         std::integral_constant<int, Pos> token,
                         typename std::enable_if<Pos < sizeof...(Ts)>::type* = nullptr)
    -> decltype(visitor(none_t{})) {
        return visitor(from.get(token));
    }

    template<typename Self, typename Visitor, int Pos>
    static auto do_visit(Self&,
                         const Visitor& visitor,
                         std::integral_constant<bool, true> /* is_void */,
                         std::integral_constant<int, Pos>,
                         typename std::enable_if<Pos < sizeof...(Ts)>::type* = nullptr)
    -> decltype(visitor(none_t{})) {
        return visitor();
    }

    template<typename Self, typename Visitor, int Pos>
    static auto do_visit(Self&,
                         const Visitor& visitor,
                         std::integral_constant<bool, false>,
                         std::integral_constant<int, Pos>,
                         typename std::enable_if<Pos >= sizeof...(Ts)>::type* = nullptr)
    -> decltype(visitor(none_t{})) {
        // this is not the function you are looking for
        throw std::runtime_error("invalid type found");
    }

    template<typename Self, typename Visitor>
    static auto do_apply(Self& from, const Visitor& visitor) -> decltype(visitor(none_t{})) {
        switch (from.m_type) {
            default: throw std::runtime_error("invalid type found");
            case -1: return visitor(none_t{});
            CPPA_OPTIONAL_VARIANT_CASE(0);
            CPPA_OPTIONAL_VARIANT_CASE(1);
            CPPA_OPTIONAL_VARIANT_CASE(2);
            CPPA_OPTIONAL_VARIANT_CASE(3);
            CPPA_OPTIONAL_VARIANT_CASE(4);
            CPPA_OPTIONAL_VARIANT_CASE(5);
            CPPA_OPTIONAL_VARIANT_CASE(6);
            CPPA_OPTIONAL_VARIANT_CASE(7);
            CPPA_OPTIONAL_VARIANT_CASE(8);
            CPPA_OPTIONAL_VARIANT_CASE(9);
        }
    }

    inline void destroy_data() {
        apply(detail::optional_variant_data_destructor{});
    }

    template<typename U>
    typename std::enable_if<
        !std::is_same<typename util::rm_const_and_ref<U>::type, none_t>::value
    >::type
    set(U&& arg) {
        typedef typename util::rm_const_and_ref<U>::type stripped_type;
        typedef typename detail::unlift_void<stripped_type>::type type;
        static constexpr int type_id = util::tl_find<types, type>::value;
        static_assert(type_id != -1, "invalid type");
        m_type = type_id;
        auto& ref = m_data.get(make_int_token<type_id>());
        new (&ref) stripped_type (std::forward<U>(arg));
    }

    inline void set(const none_t&) { m_type = -1; }

    int m_type;
    detail::optional_variant_data<typename detail::lift_void<Ts>::type...> m_data;

};

/**
 * @relates optional_variant
 */
template<typename T, typename... Us>
const T& get(const optional_variant<Us...>& value) {
    std::integral_constant<int, util::tl_find<util::type_list<Us...>, T>::value> token;
    return value.get(token);
}

/**
 * @relates optional_variant
 */
template<typename T, typename... Us>
T& get_ref(optional_variant<Us...>& value) {
    std::integral_constant<int, util::tl_find<util::type_list<Us...>, T>::value> token;
    return value.get(token);
}

/**
 * @relates optional_variant
 */
template<typename Visitor, typename... Ts>
auto apply_visitor(const Visitor& visitor, const optional_variant<Ts...>& data) -> decltype(visitor(none_t{})) {
    return data.apply(visitor);
}

/**
 * @relates optional_variant
 */
template<typename Visitor, typename... Ts>
auto apply_visitor(const Visitor& visitor, optional_variant<Ts...>& data) -> decltype(visitor(none_t{})) {
    return data.apply(visitor);
}

} // namespace cppa

#endif // OPTIONAL_VARIANT_HPP