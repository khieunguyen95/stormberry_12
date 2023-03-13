import 'package:postgres/postgres.dart';
import 'base_repository.dart';

abstract class ModelRepositoryUpdate<UpdateRequest> {
  Future<PostgreSQLResult?> updateOne(UpdateRequest request);
  Future<PostgreSQLResult?> updateMany(List<UpdateRequest> requests);
}

mixin RepositoryUpdateMixin<UpdateRequest> on BaseRepository
    implements ModelRepositoryUpdate<UpdateRequest> {
  @override
  Future<PostgreSQLResult?> updateOne(UpdateRequest request) => transaction(() {
        try {
          return update([request]);
        } catch (e) {
          return null;
        }
      });
  @override
  Future<PostgreSQLResult?> updateMany(List<UpdateRequest> requests) =>
      transaction(
        () {
          try {
            return update(requests);
          } catch (e) {
            return null;
          }
        },
      );

  Future<PostgreSQLResult?> update(List<UpdateRequest> requests);
}
