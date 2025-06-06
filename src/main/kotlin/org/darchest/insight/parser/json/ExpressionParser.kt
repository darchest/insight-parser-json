/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.parser.json

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.darchest.insight.*
import org.darchest.insight.vendor.postgresql.PostgresArrayContains
import org.darchest.insight.vendor.postgresql.PostgresILike
import org.darchest.insight.vendor.postgresql.PostgresInOperator
import org.darchest.insight.vendor.postgresql.UUIDArray
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*

typealias JsonExprOperationHandler = (dataSource: SqlDataSource, jsonExpression: JsonObject) -> SqlValue<*, *>?

object ExpressionParser {
    /*
    { prop: <codeName>, op: <operator = eq>, value: <value> }
    { op: <or/and>, exprs: [<expr1>, ..., ,exprN>] }
     */

    private val df: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.of("Europe/Moscow"))

    private val opHandlers = mutableMapOf<String,JsonExprOperationHandler>()

    @Suppress("UNCHECKED_CAST")
    private val columnFillers = mutableMapOf<Class<*>, ((TableColumn<*, *>, JsonElement) -> Unit)>(
        String::class.java to { c, json -> (c as TableColumn<String, *>).invoke(json.asString) },
        UUID::class.java to { c, json -> (c as TableColumn<UUID, *>).invoke(UUID.fromString(json.asString)) },
        Long::class.java to { c, json -> (c as TableColumn<Long, *>).invoke(json.asLong) },
        Boolean::class.java to { c, json -> (c as TableColumn<Boolean, *>).invoke(json.asBoolean) },
        Instant::class.java to { c, json -> (c as TableColumn<Instant, *>).invoke(if (json.asString.isEmpty()) Instant.ofEpochMilli(Long.MIN_VALUE) else df.parse(json.asString).query(Instant::from)) },
        UUIDArray::class.java to { c, json -> (c as TableColumn<UUIDArray, *>).invoke(UUIDArray(JsonParser.parseString(json.asString).asJsonArray.map { it.asUUID })) },
    )

    init {
        registerOpHandler("and", ::opAndOrHandler)
        registerOpHandler("or", ::opAndOrHandler)
        registerOpHandler("ilike", ::opIlikeHandler)
        registerOpHandler("arr_in", ::opArrInHandler)
    }

    fun parseWhere(dataSource: SqlDataSource, jsonExpression: JsonElement): SqlValue<*, *>? {
        if (jsonExpression.isJsonObject)
            return parseWhereObject(dataSource, jsonExpression.asJsonObject)
        else if (jsonExpression.isJsonArray) {
            val jsonArr = jsonExpression.asJsonArray
            if (jsonArr.isEmpty)
                return null
            val exprs = jsonArr.mapNotNull { el -> parseWhereObject(dataSource, el.asJsonObject) }
            return dataSource.vendor().createLogicalOperation(LogicalOperation.Operator.AND, exprs)
        }
        throw RuntimeException("Incorrect where $jsonExpression")
    }

    fun parseSortRules(dataSource: SqlDataSource, rules: JsonArray): List<SortInfo> {
        return rules.map { parseSortRule(dataSource, it.asJsonObject) }
    }

    fun fillTable(table: Table, data: JsonObject) {
        data.keySet().forEach { key ->
            val col = table.columns().find { c -> c.codeName == key }
            if (col == null)
                return@forEach
            fillColumn(col, data[key])
        }
    }

    fun fillColumn(column: TableColumn<*, *>, value: JsonElement) {
        columnFillers[column.javaClass]!!.invoke(column, value)
    }

    fun registerOpHandler(op: String, opHandler: JsonExprOperationHandler) {
        opHandlers[op] = opHandler
    }

    fun registerColumnFiller(cls: Class<*>, columnFiller: ((TableColumn<*, *>, JsonElement) -> Unit)) {
        columnFillers[cls] = columnFiller
    }


    private fun parseWhereObject(dataSource: SqlDataSource, jsonExpression: JsonObject): SqlValue<*, *>? {
        val op = if (jsonExpression.has("op")) jsonExpression.get("op").asString.lowercase(Locale.getDefault()) else "eq"
        val opHandler = opHandlers.getOrDefault(op, ::elseHandler)
        return opHandler(dataSource, jsonExpression)
    }
    
    private fun opAndOrHandler(dataSource: SqlDataSource, jsonExpression: JsonObject): SqlValue<*, *>? {
        val op = jsonExpression.get("op").asString
        val exprs = jsonExpression.getAsJsonArray("exprs")

        val enumOp = LogicalOperation.Operator.valueOf(op.lowercase(Locale.getDefault()))
        val exprsObj = exprs.mapNotNull { el -> parseWhereObject(dataSource, el.asJsonObject) }
        if (exprsObj.isEmpty())
            return null

        return dataSource.vendor().createLogicalOperation(enumOp, exprsObj)
    }

    private fun opIlikeHandler(dataSource: SqlDataSource, jsonExpression: JsonObject): SqlValue<*, *> {
        val prop = jsonExpression.get("prop").asString
        val dsProp = dataSource.sqlValueByCodeName(prop) ?: throw RuntimeException("$prop is undefined")

        val jsonValue = jsonExpression.get("value")
        return PostgresILike(dsProp, SqlConst("%${jsonValue.asString}%", String::class.java, dsProp.sqlType))
    }

    private fun opArrInHandler(dataSource: SqlDataSource, jsonExpression: JsonObject): SqlValue<*, *> {
        val prop = jsonExpression.get("prop").asString
        val dsProp = dataSource.sqlValueByCodeName(prop) ?: throw RuntimeException("$prop is undefined")

        val jsonValue = jsonExpression.get("value")

        val arr = UUIDArray()
        if (jsonValue.isJsonArray)
            arr.addAll(jsonValue.asJsonArray.map { it.asUUID })
        else
            arr.add(jsonValue.asUUID)
        return PostgresArrayContains(dsProp, SqlConst(arr, UUIDArray::class.java, dsProp.sqlType))
    }

    private fun elseHandler(dataSource: SqlDataSource, jsonExpression: JsonObject): SqlValue<*, *> {
        val prop = jsonExpression.get("prop").asString
        val dsProp = dataSource.sqlValueByCodeName(prop) ?: throw RuntimeException("$prop is undefined")

        val op = if (jsonExpression.has("op")) jsonExpression.get("op").asString else "eq"
        val jsonValue = jsonExpression.get("value")

        val enumOp = ComparisonOperation.Operator.valueOf(op.lowercase(Locale.getDefault()))

        if (op == "eq" && jsonValue.isJsonArray) {
            val consts = jsonValue.asJsonArray.map { j -> SqlConst(j.asString, String::class.java, dsProp.sqlType) }
            return PostgresInOperator(dsProp, consts)
        } else if (op == "neq" && jsonValue.isJsonArray) {
            val consts = jsonValue.asJsonArray.map { j -> SqlConst(j.asString, String::class.java, dsProp.sqlType) }
            return PostgresInOperator(dsProp, consts, true)
        } else {
            val value = jsonValue.asString
            val valueObj = SqlConst(value, String::class.java, dsProp.sqlType)
            return dataSource.vendor().createComparisonOperation(dsProp, enumOp, valueObj)
        }
    }

    private fun parseSortRule(dataSource: SqlDataSource, rule: JsonObject): SortInfo {
        if (rule.has("prop") && rule.has(("dir"))) {
            val prop = rule.get("prop").asString
            val field = dataSource.sqlValueByCodeName(prop)
                ?: throw RuntimeException("Incorrect sort rule '${rule}': prop '$prop' isn't found")
            return SortInfo(field, SortInfo.Direction.valueOf(rule.get("dir").asString.lowercase(Locale.getDefault())))
        }
        throw RuntimeException("Incorrect sort rule '${rule}': keys 'prop' and 'dir' required")
    }
}