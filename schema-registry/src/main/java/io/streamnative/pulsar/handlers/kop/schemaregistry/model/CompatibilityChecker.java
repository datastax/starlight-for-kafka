package io.streamnative.pulsar.handlers.kop.schemaregistry.model;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class CompatibilityChecker {

    public enum Mode {
        NONE,
        BACKWARD,
        BACKWARD_TRANSITIVE,
        FORWARD,
        FORWARD_TRANSITIVE,
        FULL,
        FULL_TRANSITIVE
    }

    public static CompletableFuture<Boolean> verify(Schema schema, String subject, SchemaStorage schemaStorage) {
        CompletableFuture<CompatibilityChecker.Mode> mode = schemaStorage.getCompatibilityMode(subject);
        return mode.thenCompose(m -> {
            if (m == Mode.NONE) {
                return CompletableFuture.completedFuture(true);
            }
            CompletableFuture<List<Integer>> versions = schemaStorage.getAllVersionsForSubject(subject);
            return versions.thenCompose(vv -> {
                if (vv.isEmpty()) {
                    // no versions ?
                    return CompletableFuture.completedFuture(false);
                }
               final List<Integer> versionsToCheck;
               if (m == Mode.BACKWARD || m == Mode.FORWARD) {
                   // only latest
                   versionsToCheck = Arrays.asList(vv.stream().mapToInt(Integer::intValue).max().getAsInt());
               } else {
                   // all the versions
                   versionsToCheck = vv;
               }
               log.info("Compare schema against {} versions", versionsToCheck.size());
               CompletableFuture<Boolean> res = new CompletableFuture<>();

               @AllArgsConstructor
               class HandleSchema implements BiConsumer<Schema, Throwable> {

                   final int index;

                   public void accept(Schema downloadedSchema, Throwable err) {
                       if (err != null) {
                           res.completeExceptionally(err);
                       } else {
                           boolean isCompatible = CompatibilityChecker
                                   .isCompatible(schema, downloadedSchema,
                                           m);
                           if (!isCompatible) {
                               res.complete(false);
                               return;
                           }
                           if (index == versionsToCheck.size() -1 ) {
                               res.complete(true);
                               return;
                           }
                           // recursion
                           int id = versionsToCheck.get(index);
                           schemaStorage
                                   .findSchemaById(id)
                                   .whenComplete(new HandleSchema(index + 1));

                       }
                   }
               }

               // verify first
               int id = versionsToCheck.get(0);
               schemaStorage
                       .findSchemaById(id)
                       .whenComplete(new HandleSchema(0));

               return res;
            });
        });
    }

    public static boolean isCompatible(Schema schema1, Schema schema2, Mode mode) {
        switch (mode) {
            case BACKWARD:
            case BACKWARD_TRANSITIVE:
                    return isBackwardCompatible(schema1, schema2);
            case FORWARD:
            case FORWARD_TRANSITIVE:
                return isForwardCompatible(schema1, schema2);

            case FULL:
            case FULL_TRANSITIVE:
                return isForwardCompatible(schema1, schema2)
                        && isBackwardCompatible(schema1, schema2);
            case NONE:
                return true;
            default:
                throw new IllegalStateException();
        }
    }

    public static boolean isBackwardCompatible(Schema schema1, Schema schema2) {
        return true;
    }

    public static boolean isForwardCompatible(Schema schema1, Schema schema2) {
        return true;
    }


}
