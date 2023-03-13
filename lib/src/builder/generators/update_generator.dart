import '../elements/column/column_element.dart';
import '../elements/column/field_column_element.dart';
import '../elements/column/foreign_column_element.dart';
import '../elements/column/reference_column_element.dart';
import '../elements/table_element.dart';
import '../utils.dart';

class UpdateGenerator {
  String generateUpdateMethod(TableElement table) {
    var deepUpdates = <String>[];

    for (var column in table.columns
        .whereType<ReferenceColumnElement>()
        .where((c) => c.linkedTable.primaryKeyColumn == null)) {
      if (column.linkedTable.columns
          .where((c) =>
              c is ForeignColumnElement &&
              c.linkedTable != table &&
              !c.isNullable)
          .isNotEmpty) {
        continue;
      }

      if (!column.isList) {
        var requestParams = <String>[];
        for (var c
            in column.linkedTable.columns.whereType<ParameterColumnElement>()) {
          if (c is ForeignColumnElement) {
            if (c.linkedTable == table) {
              requestParams.add(
                  '${c.paramName}: r.${table.primaryKeyColumn!.paramName}');
            }
          } else {
            requestParams
                .add('${c.paramName}: r.${column.paramName}!.${c.paramName}');
          }
        }

        var deepUpdate = '''
          await db.${column.linkedTable.repoName}.updateMany(requests.where((r) => r.${column.paramName} != null).map((r) {
            return ${column.linkedTable.element.name}UpdateRequest(${requestParams.join(', ')});
          }).toList());
        ''';

        deepUpdates.add(deepUpdate);
      } else {
        var requestParams = <String>[];
        for (var c
            in column.linkedTable.columns.whereType<ParameterColumnElement>()) {
          if (c is ForeignColumnElement) {
            if (c.linkedTable == table) {
              requestParams.add(
                  '${c.paramName}: r.${table.primaryKeyColumn!.paramName}');
            }
          } else {
            requestParams.add('${c.paramName}: rr.${c.paramName}');
          }
        }

        var deepUpdate = '''
          await db.${column.linkedTable.repoName}.updateMany(requests.where((r) => r.${column.paramName} != null).expand((r) {
            return r.${column.paramName}!.map((rr) => ${column.linkedTable.element.name}UpdateRequest(${requestParams.join(', ')}));
          }).toList());
        ''';

        deepUpdates.add(deepUpdate);
      }
    }

    var hasPrimaryKey = table.primaryKeyColumn != null;
    var setColumns = table.columns
        .whereType<NamedColumnElement>()
        .where((c) =>
            (hasPrimaryKey
                ? c != table.primaryKeyColumn
                : c is FieldColumnElement) &&
            (c is! FieldColumnElement || !c.isAutoIncrement))
        .toList();

    var indexesNeedRemove = <int>[];
    for (var i = 0; i < setColumns.length - 1; i++) {
      var itemContain = 0;
      var parent = setColumns[i];
      for (var j = 1; j < setColumns.length; j++) {
        var child = setColumns[j];
        if (parent.columnName == child.columnName) {
          itemContain++;
        }
        if (itemContain > 1) {
          indexesNeedRemove.add(j);
          break;
        }
      }
    }
    indexesNeedRemove = indexesNeedRemove.toSet().toList();
    while (indexesNeedRemove.length > 0) {
      setColumns.removeAt(indexesNeedRemove.removeLast());
    }

    var updateColumns = table.columns
        .whereType<NamedColumnElement>()
        .where((c) =>
            table.primaryKeyColumn == c ||
            c is! FieldColumnElement ||
            !c.isAutoIncrement)
        .toList();
    indexesNeedRemove = <int>[];
    for (var i = 0; i < updateColumns.length - 1; i++) {
      var itemContain = 0;
      var parent = updateColumns[i];
      for (var j = 1; j < updateColumns.length; j++) {
        var child = updateColumns[j];
        if (parent.columnName == child.columnName) {
          itemContain++;
        }
        if (itemContain > 1) {
          indexesNeedRemove.add(j);
          break;
        }
      }
    }
    indexesNeedRemove = indexesNeedRemove.toSet().toList();
    while (indexesNeedRemove.length > 0) {
      updateColumns.removeAt(indexesNeedRemove.removeLast());
    }

    String toUpdateValue(NamedColumnElement c) {
      if (c.converter != null) {
        return '\${values.add(${c.converter!.toSource()}.tryEncode(r.${c.paramName}))}';
      } else {
        return '\${values.add(r.${c.paramName})}';
      }
    }

    String whereClause;

    if (hasPrimaryKey) {
      whereClause =
          '"${table.tableName}"."${table.primaryKeyColumn!.columnName}" = UPDATED."${table.primaryKeyColumn!.columnName}"';
    } else {
      whereClause = table.columns
          .whereType<ForeignColumnElement>()
          .map((c) =>
              '"${table.tableName}"."${c.columnName}" = UPDATED."${c.columnName}"')
          .join(' AND ');
    }

    return '''
        @override
        Future<PostgreSQLResult?> update(List<${table.element.name}UpdateRequest> requests) async {
          if (requests.isEmpty) return null;
          var values = QueryValues();
          return db.query(
            'UPDATE "${table.tableName}"\\n'
            'SET ${setColumns.map((c) => '"${c.columnName}" = COALESCE(UPDATED."${c.columnName}"::${c.rawSqlType}, "${table.tableName}"."${c.columnName}")').join(', ')}\\n'
            'FROM ( VALUES \${requests.map((r) => '( ${updateColumns.map(toUpdateValue).join(', ')} )').join(', ')} )\\n'
            'AS UPDATED(${updateColumns.map((c) => '"${c.columnName}"').join(', ')})\\n'
            'WHERE $whereClause',
            values.values,
          );
          ${deepUpdates.isNotEmpty ? deepUpdates.join() : ''}
        }
      ''';
  }

  String generateUpdateRequest(TableElement table) {
    var requestClassName = '${table.element.name}UpdateRequest';
    var requestFields = <MapEntry<String, String>>[];

    for (var column in table.columns) {
      if (column is FieldColumnElement) {
        if (column == table.primaryKeyColumn || !column.isAutoIncrement) {
          requestFields.add(MapEntry(
            column.parameter.type.getDisplayString(withNullability: false) +
                (column == table.primaryKeyColumn ? '' : '?'),
            column.paramName,
          ));
        }
      } else if (column is ReferenceColumnElement &&
          column.linkedTable.primaryKeyColumn == null) {
        if (column.linkedTable.columns
            .where((c) =>
                c is ForeignColumnElement &&
                c.linkedTable != table &&
                !c.isNullable)
            .isNotEmpty) {
          continue;
        }
        requestFields.add(MapEntry(
            column.parameter!.type.getDisplayString(withNullability: false) +
                (column == table.primaryKeyColumn ? '' : '?'),
            column.paramName));
      } else if (column is ForeignColumnElement) {
        var fieldNullSuffix = column == table.primaryKeyColumn ? '' : '?';
        String fieldType;
        if (column.linkedTable.primaryKeyColumn == null) {
          fieldType = column.linkedTable.element.name;
          if (column.isList) {
            fieldType = 'List<$fieldType>';
          }
        } else {
          fieldType = column.linkedTable.primaryKeyColumn!.dartType;
        }
        requestFields
            .add(MapEntry('$fieldType$fieldNullSuffix', column.paramName));
      }
    }

    var indexesNeedRemove = <int>[];
    for (var i = 0; i < requestFields.length - 1; i++) {
      var itemContain = 0;
      var parent = requestFields[i];
      for (var j = 1; j < requestFields.length; j++) {
        var child = requestFields[j];
        if (parent.value == child.value) {
          itemContain++;
        }
        if (itemContain > 1) {
          indexesNeedRemove.add(j);
          break;
        }
      }
    }
    indexesNeedRemove = indexesNeedRemove.toSet().toList();
    while (indexesNeedRemove.length > 0) {
      requestFields.removeAt(indexesNeedRemove.removeLast());
    }

    final constructorParameters = requestFields
        .map((f) => '${f.key.endsWith('?') ? '' : 'required '}this.${f.value},')
        .join(' ');

    return '''
      ${defineClassWithMeta(requestClassName, table.meta?.read('update'))}
        $requestClassName(${constructorParameters.isNotEmpty ? '{$constructorParameters}' : ''});
        
        ${requestFields.map((f) => '${f.key} ${f.value};').join('\n')}
      }
    ''';
  }
}
