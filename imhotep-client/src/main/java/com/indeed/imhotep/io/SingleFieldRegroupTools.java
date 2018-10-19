package com.indeed.imhotep.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper classes for special regrouping case: all rules correspond to one field (most frequent case actually)
 */
public class SingleFieldRegroupTools {

    private SingleFieldRegroupTools() {
    }

    /**
     * Field properties that are common for all {@link GroupMultiRemapRule}
     */
    public static class FieldOptions {
        public final String field;
        public final boolean intType;
        public final boolean inequality;

        public FieldOptions(
                final String field,
                final boolean intType,
                final boolean inequality) {
            this.field = field;
            this.intType = intType;
            this.inequality = inequality;
        }
    }

    /**
     * Interface for gathering rules.
     */
    public interface SingleFieldRulesBuilder {

        void addIntRule(
                final int targetGroup,
                final int negativeGroup,
                final int[] positiveGroups,
                final long[] intTerms);

        void addStringRule(
                final int targetGroup,
                final int negativeGroup,
                final int[] positiveGroups,
                final String[] stringTerms);

        // Convert rules into GroupMultiRemapRule and save into collection
        class Simple implements SingleFieldRulesBuilder {
            private final List<GroupMultiRemapRule> rules = new ArrayList<>();
            private final FieldOptions options;

            public Simple(final FieldOptions options) {
                this.options = options;
            }

            @Override
            public void addIntRule(
                    final int targetGroup,
                    final int negativeGroup,
                    final int[] positiveGroups,
                    final long[] intTerms) {
                Preconditions.checkState(options.intType, "Can't add int rule for string field");
                checkParams(positiveGroups, intTerms, null);

                final RegroupCondition[] conditions = new RegroupCondition[intTerms.length];
                for (int i = 0; i < intTerms.length; i++) {
                    conditions[i] = RegroupCondition.intCondition(options.field, intTerms[i], options.inequality);
                }

                rules.add(new GroupMultiRemapRule(targetGroup, negativeGroup, positiveGroups, conditions));
            }

            @Override
            public void addStringRule(
                    final int targetGroup,
                    final int negativeGroup,
                    final int[] positiveGroups,
                    final String[] stringTerms) {
                Preconditions.checkState(!options.intType, "Can't add string rule for int field");
                checkParams(positiveGroups, null, stringTerms);

                final RegroupCondition[] conditions = new RegroupCondition[stringTerms.length];
                for (int i = 0; i < stringTerms.length; i++) {
                    conditions[i] = RegroupCondition.stringCondition(options.field, stringTerms[i], options.inequality);
                }

                rules.add(new GroupMultiRemapRule(targetGroup, negativeGroup, positiveGroups, conditions));
            }

            public GroupMultiRemapRule[] getRules() {
                return rules.toArray(new GroupMultiRemapRule[0]);
            }
        }

        // Save rules into collection of SingleFieldMultiRemapRule
        class SingleField implements SingleFieldRulesBuilder {
            private final List<SingleFieldMultiRemapRule> rules = new ArrayList<>();

            @Override
            public void addIntRule(
                    final int targetGroup,
                    final int negativeGroup,
                    final int[] positiveGroups,
                    final long[] intTerms) {
                rules.add(new SingleFieldMultiRemapRule(targetGroup, negativeGroup, positiveGroups, intTerms, null));
            }

            @Override
            public void addStringRule(
                    final int targetGroup,
                    final int negativeGroup,
                    final int[] positiveGroups,
                    final String[] stringTerms) {
                rules.add(new SingleFieldMultiRemapRule(targetGroup, negativeGroup, positiveGroups, null, stringTerms));
            }

            public SingleFieldMultiRemapRule[] getRules() {
                return rules.toArray(new SingleFieldMultiRemapRule[0]);
            }
        }

        // Encode rule into memory data stream.
        class Cached implements SingleFieldRulesBuilder {

            private int rulesCount;
            private final RequestTools.HackedByteArrayOutputStream cachedRules;

            private final FieldOptions options;

            public Cached(final FieldOptions options) {
                this.options = options;
                cachedRules = new RequestTools.HackedByteArrayOutputStream();
            }

            @Override
            public void addIntRule(
                    final int targetGroup,
                    final int negativeGroup,
                    final int[] positiveGroups,
                    final long[] intTerms) {
                checkParams(positiveGroups, intTerms, null);
                final GroupMultiRemapMessage message = marshalSingleFieldRule(targetGroup, negativeGroup, positiveGroups, intTerms, null, options);
                addMessage(message);
            }

            @Override
            public void addStringRule(
                    final int targetGroup,
                    final int negativeGroup,
                    final int[] positiveGroups,
                    final String[] stringTerms) {
                checkParams(positiveGroups, null, stringTerms);
                final GroupMultiRemapMessage message = marshalSingleFieldRule(targetGroup, negativeGroup, positiveGroups, null, stringTerms, options);
                addMessage(message);
            }

            public RequestTools.GroupMultiRemapRuleSender createSender() {
                final byte[] asBytes = cachedRules.getBuffer();
                final int len = cachedRules.getCount();
                return new RequestTools.GroupMultiRemapRuleSender.Cached(asBytes, len, rulesCount);
            }

            private void addMessage(final GroupMultiRemapMessage message) {
                try {
                    ImhotepProtobufShipping.sendProtobufNoFlush(message, cachedRules);
                    rulesCount++;
                } catch (final IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    public static class SingleFieldMultiRemapRule {
        public final int targetGroup;
        public final int negativeGroup;
        public final int[] positiveGroups;
        public final long[] intTerms;
        public final String[] stringTerms;

        public SingleFieldMultiRemapRule(
                final int targetGroup,
                final int negativeGroup,
                final int[] positiveGroups,
                final long[] intTerms,
                final String[] stringTerms) {
            checkParams(positiveGroups, intTerms, stringTerms);
            this.targetGroup = targetGroup;
            this.positiveGroups = positiveGroups;
            this.negativeGroup = negativeGroup;
            this.intTerms = intTerms;
            this.stringTerms = stringTerms;
        }
    }

    public static GroupMultiRemapRule createMultiRule(
            final SingleFieldMultiRemapRule rule,
            final FieldOptions options) {
        final RegroupCondition[] conditions = new RegroupCondition[rule.positiveGroups.length];
        if (options.intType) {
            for (int i = 0; i < rule.intTerms.length; i++) {
                conditions[i] = RegroupCondition.intCondition(options.field, rule.intTerms[i], options.inequality);
            }
        } else {
            for (int i = 0; i < rule.stringTerms.length; i++) {
                conditions[i] = RegroupCondition.stringCondition(options.field, rule.stringTerms[i], options.inequality);
            }
        }
        return new GroupMultiRemapRule(rule.targetGroup, rule.negativeGroup, rule.positiveGroups, conditions);
    }


    private static void checkParams(
            final int[] positiveGroups,
            final long[] intTerms,
            final String[] stringTerms) {
        Preconditions.checkState((intTerms == null) != (stringTerms == null), "Expected only int terms or string terms be defined");
        final int length = (intTerms != null) ? intTerms.length : stringTerms.length;
        Preconditions.checkArgument(length == positiveGroups.length, "positiveGroups.length must equal length of terms");
        if (stringTerms != null) {
            for (final String term : stringTerms) {
                Preconditions.checkArgument(term != null, "cannot have null string term");
            }
        }
    }

    public static GroupMultiRemapMessage marshalSingleFieldRule(
            final int targetGroup,
            final int negativeGroup,
            final int[] positiveGroups,
            final long[] intTerms,
            final String[] stringTerms,
            final FieldOptions options) {
        final GroupMultiRemapMessage.Builder builder = GroupMultiRemapMessage.newBuilder();
        builder.setNegativeGroup(negativeGroup).setTargetGroup(targetGroup);
        final int numConditions = positiveGroups.length;
        for (int conditionIx = 0; conditionIx < numConditions; conditionIx++) {
            builder.addCondition(marshalRegroupCondition(intTerms, stringTerms, options, conditionIx));
            builder.addPositiveGroup(positiveGroups[conditionIx]);
        }
        return builder.build();
    }

    public static GroupMultiRemapMessage[] marshal(final SingleFieldMultiRemapRule[] rules, final FieldOptions options) {
        final GroupMultiRemapMessage[] result = new GroupMultiRemapMessage[rules.length];
        for (int i = 0; i < rules.length; i++) {
            result[i] = marshal(rules[i], options);
        }
        return result;
    }

    public static GroupMultiRemapMessage marshal(final SingleFieldMultiRemapRule rule, final FieldOptions options) {
        final GroupMultiRemapMessage.Builder builder = GroupMultiRemapMessage.newBuilder();
        builder.setNegativeGroup(rule.negativeGroup).setTargetGroup(rule.targetGroup);
        final int numConditions = rule.positiveGroups.length;
        for (int conditionIx = 0; conditionIx < numConditions; conditionIx++) {
            builder.addCondition(marshalRegroupCondition(rule.intTerms, rule.stringTerms, options, conditionIx));
            builder.addPositiveGroup(rule.positiveGroups[conditionIx]);
        }
        return builder.build();
    }

    public static RegroupConditionMessage marshalRegroupCondition(
            final long[] intTerms,
            final String[] stringTerms,
            final FieldOptions options,
            final int termIndex) {
        final RegroupConditionMessage.Builder builder = RegroupConditionMessage.newBuilder()
                .setField(options.field)
                .setIntType(options.intType);
        if (options.intType) {
            builder.setIntTerm(intTerms[termIndex]);
        } else {
            builder.setStringTerm(stringTerms[termIndex]);
        }
        if (options.inequality) {
            builder.setInequality(true);
        }
        return builder.build();
    }
}
