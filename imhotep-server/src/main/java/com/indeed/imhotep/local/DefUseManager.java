package com.indeed.imhotep.local;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manages the Definitions and Usages for all named Group.
 * A named Group is defined whenever an Imhotep Command uses it for the first time, or if re-used after deletion.
 * Each successive ImhotepCommand using it as it's input Group will add to it's usage.
 */
public class DefUseManager {

    private final Map<String, DefUseList> defUseListMap = new HashMap<>();

    public static DefUseList getDefaultDefUseListForGroup() {
        return new DefUseList(Futures.immediateFuture(null), new ArrayList<>());
    }

    // returns a list of futures which should be executed before the gives set of input and output Groups.
    public List<ListenableFuture<Object>> getUpstreamFutures(final List<String> inputGroups, final List<String> outputGroups) {
        final List<ListenableFuture<Object>> dependentFutures = inputGroups.stream().map(this::getDef).collect(Collectors.toList());
        outputGroups.forEach(outputGroup -> {
            if (defUseListMap.containsKey(outputGroup)) {
                dependentFutures.addAll(getDefAndUses(outputGroup));
            }
        });
        return dependentFutures;
    }

    private ListenableFuture<Object> getDef(final String groupName) {
        defUseListMap.putIfAbsent(groupName, getDefaultDefUseListForGroup());
        return defUseListMap.get(groupName).def;
    }

    private List<ListenableFuture<Object>> getDefAndUses(final String groupName) {
        return defUseListMap.get(groupName).getDefAndUse();
    }

    public void addUses(final List<String> groupNames, final ListenableFuture<Object> usingFuture) {
        for (final String groupName : groupNames) {
            Preconditions.checkArgument(defUseListMap.containsKey(groupName), "Group " + groupName + "doesn't exist.");
            defUseListMap.get(groupName).uses.add(usingFuture);
        }
    }

    public void addDefinitions(final List<String> outputGroups, final ListenableFuture<Object> commandFuture) {
        outputGroups.forEach(outputGroup -> defUseListMap.put(outputGroup, new DefUseList(commandFuture, new ArrayList<>())));
    }

    public List<ListenableFuture<Object>> getAllDefsUses() {
        return defUseListMap.values().parallelStream().map(DefUseList::getDefAndUse).flatMap(List::stream).collect(Collectors.toList());
    }

    private static class DefUseList {
        private final ListenableFuture<Object> def;
        private final List<ListenableFuture<Object>> uses;

        DefUseList(final ListenableFuture def, final List<ListenableFuture<Object>> uses) {
            this.def = def;
            this.uses = uses;
        }

        public List<ListenableFuture<Object>> getDefAndUse() {
            return Stream.concat(Stream.of(def), uses.stream()).collect(Collectors.toList());
        }
    }
}
