import '../core/query_params.dart';
import 'base_repository.dart';
import 'package:postgres/postgres.dart';

abstract class ModelRepositoryDelete<DeleteRequest> {
  Future<PostgreSQLResult?> deleteOne(DeleteRequest id);
  Future<PostgreSQLResult?> deleteMany(List<DeleteRequest> ids);
}

mixin RepositoryDeleteMixin<DeleteRequest> on BaseRepository
    implements ModelRepositoryDelete<DeleteRequest> {
  Future<PostgreSQLResult?> delete(List<DeleteRequest> keys) async {
    if (keys.isEmpty) return null;
    var values = QueryValues();
    return db.query(
      'DELETE FROM "$tableName"\n'
      'WHERE "$tableName"."$keyName" IN ( ${keys.map((k) => values.add(k)).join(', ')} )',
      values.values,
    );
  }

  @override
  Future<PostgreSQLResult?> deleteOne(DeleteRequest key) => transaction(() {
        try {
          return delete([key]);
        } catch (e) {
          return null;
        }
      });
  @override
  Future<PostgreSQLResult?> deleteMany(List<DeleteRequest> keys) =>
      transaction(() {
        try {
          return delete(keys);
        } catch (e) {
          return null;
        }
      });
}
