CREATE OR REPLACE PROCEDURE CLIENT_STAGE_T.SOURCE_TARGET_DDL_ADJUST(
    source_schema STRING,
    source_table STRING,
    target_schema STRING,
    target_table STRING,
    execute_alters BOOLEAN DEFAULT FALSE
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
function generateAlters() {
    const sourceSchema = SOURCE_SCHEMA;
    const sourceTable = SOURCE_TABLE;
    const targetSchema = TARGET_SCHEMA;
    const targetTable = TARGET_TABLE;
    const executeAlters = EXECUTE_ALTERS;
    
    let result = '';
    let hasErrors = false;
    let executedCount = 0;
    let errorCount = 0;
    
    // Log the execution start
    const actionType = executeAlters ? 'EXECUTE' : 'LOG_ONLY';
    logToTable(sourceSchema, sourceTable, targetSchema, targetTable, actionType, null, 'INFO', null, null, 'STARTED', 'Execution started');
    
    // Get source table columns
    const sourceQuery = `
        SELECT 
            UPPER(COLUMN_NAME) AS COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '${sourceSchema}' 
        AND TABLE_NAME = '${sourceTable}'
        ORDER BY ORDINAL_POSITION`;
    
    const sourceResult = snowflake.execute({sqlText: sourceQuery});
    const sourceColumns = [];
    while (sourceResult.next()) {
        sourceColumns.push({
            columnName: sourceResult.getColumnValue(1),
            dataType: sourceResult.getColumnValue(2),
            isNullable: sourceResult.getColumnValue(3),
            columnDefault: sourceResult.getColumnValue(4),
            charLength: sourceResult.getColumnValue(5),
            numericPrecision: sourceResult.getColumnValue(6),
            numericScale: sourceResult.getColumnValue(7)
        });
    }
    
    // Get target table columns
    const targetQuery = `
        SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '${targetSchema}' 
        AND TABLE_NAME = '${targetTable}'`;
    
    const targetResult = snowflake.execute({sqlText: targetQuery});
    const targetColumns = {};
    while (targetResult.next()) {
        const colName = targetResult.getColumnValue(1);
        targetColumns[colName] = {
            dataType: targetResult.getColumnValue(2),
            isNullable: targetResult.getColumnValue(3),
            charLength: targetResult.getColumnValue(4),
            numericPrecision: targetResult.getColumnValue(5),
            numericScale: targetResult.getColumnValue(6)
        };
    }
    
    // Check each source column
    for (const sourceCol of sourceColumns) {
        const targetCol = targetColumns[sourceCol.columnName];
        
        if (!targetCol) {
            // Column missing - generate ADD statement
            const alterSql = generateAddColumnSql(sourceCol, targetSchema, targetTable);
            result += alterSql + '\n';
            
            if (executeAlters) {
                try {
                    snowflake.execute({sqlText: alterSql});
                    logToTable(sourceSchema, sourceTable, targetSchema, targetTable, actionType, sourceCol.columnName, 'ADD_COLUMN', alterSql, null, 'SUCCESS', 'Column added successfully');
                    executedCount++;
                } catch (error) {
                    logToTable(sourceSchema, sourceTable, targetSchema, targetTable, actionType, sourceCol.columnName, 'ADD_COLUMN', alterSql, error.message, 'ERROR', 'Failed to add column');
                    errorCount++;
                    hasErrors = true;
                }
            } else {
                logToTable(sourceSchema, sourceTable, targetSchema, targetTable, actionType, sourceCol.columnName, 'ADD_COLUMN', alterSql, null, 'INFO', 'ALTER statement generated');
            }
        } else {
            // Check for datatype compatibility
            const compatibilityCheck = checkDataTypeCompatibility(sourceCol, targetCol);
            if (compatibilityCheck.isCompatible === false) {
                result += '-- ERROR: ' + compatibilityCheck.errorMessage + '\n';
                logToTable(sourceSchema, sourceTable, targetSchema, targetTable, actionType, sourceCol.columnName, 'ERROR', null, compatibilityCheck.errorMessage, 'ERROR', 'Datatype compatibility issue');
                hasErrors = true;
                errorCount++;
            } else {
                logToTable(sourceSchema, sourceTable, targetSchema, targetTable, actionType, sourceCol.columnName, 'INFO', null, null, 'SUCCESS', 'Datatype compatible');
            }
        }
    }
    
    // Log execution summary
    const summaryMessage = `Execution completed. ${executedCount} statements executed, ${errorCount} errors encountered.`;
    logToTable(sourceSchema, sourceTable, targetSchema, targetTable, actionType, null, 'INFO', null, null, hasErrors ? 'WARNING' : 'SUCCESS', summaryMessage);
    
    if (hasErrors) {
        result = '-- WARNING: Breaking changes detected. Review errors before executing.\n' + result;
    }
    
    if (executeAlters) {
        result = `-- EXECUTION SUMMARY:\n-- ${executedCount} statements executed\n-- ${errorCount} errors encountered\n\n` + result;
    }
    
    return result;
}

function logToTable(sourceSchema, sourceTable, targetSchema, targetTable, actionType, columnName, action, sqlStatement, errorMessage, status, details) {
    const insertQuery = `
        INSERT INTO ALTER_STATEMENTS_LOG (
            SOURCE_SCHEMA, SOURCE_TABLE, TARGET_SCHEMA, TARGET_TABLE, 
            ACTION_TYPE, COLUMN_NAME, ACTION, SQL_STATEMENT, ERROR_MESSAGE, STATUS
        ) VALUES (
            '${sourceSchema}', '${sourceTable}', '${targetSchema}', '${targetTable}',
            '${actionType}', ${columnName ? `'${columnName}'` : 'NULL'}, '${action}', 
            ${sqlStatement ? `'${sqlStatement.replace(/'/g, "''")}'` : 'NULL'}, 
            ${errorMessage ? `'${errorMessage.replace(/'/g, "''")}'` : 'NULL'}, '${status}'
        )`;
    
    snowflake.execute({sqlText: insertQuery});
}

function generateAddColumnSql(column, targetSchema, targetTable) {
    let dataType = column.dataType;
    
    // Handle different data types with their specific parameters
    if (column.dataType === 'VARCHAR' && column.charLength) {
        dataType += `(${column.charLength})`;
    } else if (column.dataType === 'CHAR' && column.charLength) {
        dataType += `(${column.charLength})`;
    } else if (column.dataType === 'NUMBER' && column.numericPrecision) {
        if (column.numericScale) {
            dataType += `(${column.numericPrecision},${column.numericScale})`;
        } else {
            dataType += `(${column.numericPrecision})`;
        }
    } else if (column.dataType === 'DECIMAL' && column.numericPrecision) {
        if (column.numericScale) {
            dataType += `(${column.numericPrecision},${column.numericScale})`;
        } else {
            dataType += `(${column.numericPrecision})`;
        }
    }
    
    const nullable = column.isNullable === 'YES' ? 'NULL' : 'NOT NULL';
    const defaultValue = column.columnDefault ? ` DEFAULT ${column.columnDefault}` : '';
    
    return `ALTER TABLE ${targetSchema}.${targetTable} ADD COLUMN ${column.columnName} ${dataType} ${nullable}${defaultValue};`;
}

function getDataTypeString(column) {
    let dataType = column.dataType;
    
    if (column.dataType === 'VARCHAR' && column.charLength) {
        dataType += `(${column.charLength})`;
    } else if (column.dataType === 'CHAR' && column.charLength) {
        dataType += `(${column.charLength})`;
    } else if (column.dataType === 'NUMBER' && column.numericPrecision) {
        if (column.numericScale) {
            dataType += `(${column.numericPrecision},${column.numericScale})`;
        } else {
            dataType += `(${column.numericPrecision})`;
        }
    } else if (column.dataType === 'DECIMAL' && column.numericPrecision) {
        if (column.numericScale) {
            dataType += `(${column.numericPrecision},${column.numericScale})`;
        } else {
            dataType += `(${column.numericPrecision})`;
        }
    }
    
    return dataType;
}

function checkDataTypeCompatibility(sourceCol, targetCol) {
    const sourceTypeStr = getDataTypeString(sourceCol);
    const targetTypeStr = getDataTypeString(targetCol);
    
    // Check NUMBER precision/scale first - this should override the hierarchy check
    if ((sourceCol.dataType === 'NUMBER' || sourceCol.dataType === 'DECIMAL') && 
        (targetCol.dataType === 'NUMBER' || targetCol.dataType === 'DECIMAL')) {
        
        // If both are NUMBER/DECIMAL, check precision and scale
        if (sourceCol.numericPrecision && targetCol.numericPrecision) {
            // Check if target can accommodate source
            if (sourceCol.numericPrecision > targetCol.numericPrecision) {
                return {
                    isCompatible: false,
                    errorMessage: `Source column ${sourceCol.columnName} has ${sourceTypeStr} which has larger precision than target ${targetTypeStr}. This would cause data truncation.`
                };
            }
            
            // Check scale compatibility
            if (sourceCol.numericScale && targetCol.numericScale && 
                sourceCol.numericScale > targetCol.numericScale) {
                return {
                    isCompatible: false,
                    errorMessage: `Source column ${sourceCol.columnName} has ${sourceTypeStr} which has larger scale than target ${targetTypeStr}. This would cause precision loss.`
                };
            }
        }
        
        // If we get here, the numeric types are compatible
        return { isCompatible: true, errorMessage: null };
    }
    
    // Check VARCHAR length
    if (sourceCol.dataType === 'VARCHAR' && targetCol.dataType === 'VARCHAR') {
        if (sourceCol.charLength && targetCol.charLength && sourceCol.charLength > targetCol.charLength) {
            return {
                isCompatible: false,
                errorMessage: `Source column ${sourceCol.columnName} has ${sourceTypeStr} which is larger than target ${targetTypeStr}. This would cause data truncation.`
            };
        }
    }
    
    // Check CHAR length
    if (sourceCol.dataType === 'CHAR' && targetCol.dataType === 'CHAR') {
        if (sourceCol.charLength && targetCol.charLength && sourceCol.charLength > targetCol.charLength) {
            return {
                isCompatible: false,
                errorMessage: `Source column ${sourceCol.columnName} has ${sourceTypeStr} which is larger than target ${targetTypeStr}. This would cause data truncation.`
            };
        }
    }
    
    // Define datatype hierarchy (larger to smaller - potential breaking changes)
    const typeHierarchy = {
        'TIMESTAMP_LTZ': ['TIMESTAMP_NTZ', 'TIMESTAMP', 'DATE'],
        'TIMESTAMP_NTZ': ['TIMESTAMP', 'DATE'],
        'TIMESTAMP': ['DATE'],
        'VARCHAR': ['CHAR', 'STRING'],
        'CHAR': ['STRING'],
        'BOOLEAN': ['BOOLEAN']
    };
    
    // Check if source datatype is larger than target datatype
    for (const [largerType, smallerTypes] of Object.entries(typeHierarchy)) {
        if (sourceCol.dataType === largerType && smallerTypes.includes(targetCol.dataType)) {
            return {
                isCompatible: false,
                errorMessage: `Source column ${sourceCol.columnName} has ${sourceTypeStr} which is larger than target ${targetTypeStr}. This would cause data truncation or loss of precision.`
            };
        }
    }
    
    // Check timestamp type compatibility
    const timestampTypes = ['TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ'];
    if (timestampTypes.includes(sourceCol.dataType) && timestampTypes.includes(targetCol.dataType)) {
        // TIMESTAMP_LTZ > TIMESTAMP_NTZ > TIMESTAMP (in terms of information content)
        if (sourceCol.dataType === 'TIMESTAMP_LTZ' && targetCol.dataType === 'TIMESTAMP_NTZ') {
            return {
                isCompatible: false,
                errorMessage: `Source column ${sourceCol.columnName} has ${sourceTypeStr} which contains timezone information that would be lost in target ${targetTypeStr}. This could cause data misinterpretation.`
            };
        }
        if (sourceCol.dataType === 'TIMESTAMP_LTZ' && targetCol.dataType === 'TIMESTAMP') {
            return {
                isCompatible: false,
                errorMessage: `Source column ${sourceCol.columnName} has ${sourceTypeStr} which contains timezone information that would be lost in target ${targetTypeStr}. This could cause data misinterpretation.`
            };
        }
        if (sourceCol.dataType === 'TIMESTAMP_NTZ' && targetCol.dataType === 'TIMESTAMP') {
            return {
                isCompatible: false,
                errorMessage: `Source column ${sourceCol.columnName} has ${sourceTypeStr} which contains no timezone info, but target ${targetTypeStr} expects timezone info. This could cause data misinterpretation.`
            };
        }
    }
    
    // Check DATE to TIMESTAMP conversion (generally safe, but warn about precision)
    if (sourceCol.dataType === 'DATE' && timestampTypes.includes(targetCol.dataType)) {
        return {
            isCompatible: true,
            errorMessage: null
        };
    }
    
    // Check TIMESTAMP to DATE conversion (potential data loss)
    if (timestampTypes.includes(sourceCol.dataType) && targetCol.dataType === 'DATE') {
        return {
            isCompatible: false,
            errorMessage: `Source column ${sourceCol.columnName} has ${sourceTypeStr} which contains time information that would be lost in target DATE. This would cause data truncation.`
        };
    }
    
    return { isCompatible: true, errorMessage: null };
}

return generateAlters();
$$;