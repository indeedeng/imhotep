package com.indeed.imhotep.local;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefUseManager {

    private final Map<String, DefUseList> defUseListMap = new HashMap<>();

    public static DefUseList getDefaultDefUseListForGroup() {
        return new DefUseList(Futures.immediateFuture(null), new ArrayList<>());
    }

    public List<ListenableFuture<Object>> getDependentFutures(final List<String> inputGroups, final List<String> outputGroups) {
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

    private List<ListenableFuture<Object>> getUses(final String groupName) {
        defUseListMap.putIfAbsent(groupName, getDefaultDefUseListForGroup());
        return defUseListMap.get(groupName).uses;
    }

    private List<ListenableFuture<Object>> getDefAndUses(final String groupName) {
        defUseListMap.putIfAbsent(groupName, getDefaultDefUseListForGroup());
        final List<ListenableFuture<Object>> defAndUses = getUses(groupName);
        defAndUses.add(getDef(groupName));
        return defAndUses;
    }

    public void addUses(final List<String> groupNames, final ListenableFuture<Object> usingFuture) {
        for (final String groupName : groupNames) {
            Preconditions.checkArgument(defUseListMap.containsKey(groupName), "Group " + groupName + "doesn't exist.");
            defUseListMap.get(groupName).uses.add(usingFuture);
        }
    }

    public void addDefinition(final List<String> outputGroups, final ListenableFuture<Object> commandFuture) {
        outputGroups.forEach(outputGroup -> defUseListMap.put(outputGroup, new DefUseList(commandFuture, new ArrayList<>())));
    }

    public List<ListenableFuture<Object>> getAllFutures(final ListenableFuture<Object> firstFuture) {
        final List<ListenableFuture<Object>> allFutures = new ArrayList<>();
        allFutures.add(firstFuture);
        defUseListMap.forEach((s, defuseList) -> {
            allFutures.addAll(defuseList.uses);
            allFutures.add(defuseList.def);
        });
        return allFutures;
    }

    private static class DefUseList {
        public final ListenableFuture<Object> def;
        public final List<ListenableFuture<Object>> uses;

        DefUseList(final ListenableFuture def, final List<ListenableFuture<Object>> uses) {
            this.def = def;
            this.uses = uses;
        }
    }
}
