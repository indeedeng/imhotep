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

    public List<ListenableFuture<Void>> getDependentFutures(final List<String> inputGroups, final List<String> outputGroups) {
        final List<ListenableFuture<Void>> dependentFutures = inputGroups.stream().map(this::getDef).collect(Collectors.toList());
        outputGroups.forEach(outputGroup -> {
            if (defUseListMap.containsKey(outputGroup)) {
                dependentFutures.addAll(getDefAndUses(outputGroup));
            }
        });
        return dependentFutures;
    }

    private ListenableFuture<Void> getDef(final String groupName) {
        defUseListMap.putIfAbsent(groupName, getDefaultDefUseListForGroup());
        return defUseListMap.get(groupName).def;
    }

    private List<ListenableFuture<Void>> getUses(final String groupName) {
        defUseListMap.putIfAbsent(groupName, getDefaultDefUseListForGroup());
        return defUseListMap.get(groupName).uses;
    }

    private List<ListenableFuture<Void>> getDefAndUses(final String groupName) {
        defUseListMap.putIfAbsent(groupName, getDefaultDefUseListForGroup());
        final List<ListenableFuture<Void>> defAndUses = getUses(groupName);
        defAndUses.add(getDef(groupName));
        return defAndUses;
    }

    public void addUses(final List<String> groupNames, final ListenableFuture<Void> usingFuture) {
        for (final String groupName : groupNames) {
            Preconditions.checkArgument(defUseListMap.containsKey(groupName), "Group " + groupName + "doesn't exist.");
            defUseListMap.get(groupName).uses.add(usingFuture);
        }
    }

    public void addDefinition(final List<String> outputGroups, final ListenableFuture<Void> commandFuture) {
        outputGroups.forEach(outputGroup -> defUseListMap.put(outputGroup, new DefUseList(commandFuture, new ArrayList<>())));
    }

    public List<ListenableFuture<Void>> getAllFutures() {
        final List<ListenableFuture<Void>> allFutures = new ArrayList<>();
        defUseListMap.forEach((s, defuseList) -> {
            allFutures.addAll(defuseList.uses);
            allFutures.add(defuseList.def);
        });
        return allFutures;
    }

    private static class DefUseList {
        public final ListenableFuture<Void> def;
        public final List<ListenableFuture<Void>> uses;

        DefUseList(final ListenableFuture def, final List<ListenableFuture<Void>> uses) {
            this.def = def;
            this.uses = uses;
        }
    }
}
