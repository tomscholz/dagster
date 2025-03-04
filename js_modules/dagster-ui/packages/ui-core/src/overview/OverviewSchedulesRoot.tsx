import {gql, useQuery} from '@apollo/client';
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogFooter,
  Heading,
  NonIdealState,
  PageHeader,
  Spinner,
  TextInput,
  Tooltip,
  colorTextLight,
} from '@dagster-io/ui-components';
import * as React from 'react';

import {PYTHON_ERROR_FRAGMENT} from '../app/PythonErrorFragment';
import {PythonErrorInfo} from '../app/PythonErrorInfo';
import {useQueryRefreshAtInterval, FIFTEEN_SECONDS} from '../app/QueryRefresh';
import {useTrackPageView} from '../app/analytics';
import {useDocumentTitle} from '../hooks/useDocumentTitle';
import {useQueryPersistedState} from '../hooks/useQueryPersistedState';
import {useSelectionReducer} from '../hooks/useSelectionReducer';
import {INSTANCE_HEALTH_FRAGMENT} from '../instance/InstanceHealthFragment';
import {INSTIGATION_STATE_FRAGMENT} from '../instigation/InstigationUtils';
import {UnloadableSchedules} from '../instigation/Unloadable';
import {filterPermissionedInstigationState} from '../instigation/filterPermissionedInstigationState';
import {ScheduleBulkActionMenu} from '../schedules/ScheduleBulkActionMenu';
import {SchedulerInfo} from '../schedules/SchedulerInfo';
import {makeScheduleKey} from '../schedules/makeScheduleKey';
import {CheckAllBox} from '../ui/CheckAllBox';
import {useFilters} from '../ui/Filters';
import {useCodeLocationFilter} from '../ui/Filters/useCodeLocationFilter';
import {useInstigationStatusFilter} from '../ui/Filters/useInstigationStatusFilter';
import {SearchInputSpinner} from '../ui/SearchInputSpinner';
import {WorkspaceContext} from '../workspace/WorkspaceContext';
import {buildRepoAddress} from '../workspace/buildRepoAddress';
import {repoAddressAsHumanString} from '../workspace/repoAddressAsString';
import {RepoAddress} from '../workspace/types';

import {BASIC_INSTIGATION_STATE_FRAGMENT} from './BasicInstigationStateFragment';
import {OverviewScheduleTable} from './OverviewSchedulesTable';
import {OverviewTabs} from './OverviewTabs';
import {sortRepoBuckets} from './sortRepoBuckets';
import {BasicInstigationStateFragment} from './types/BasicInstigationStateFragment.types';
import {
  OverviewSchedulesQuery,
  OverviewSchedulesQueryVariables,
  UnloadableSchedulesQuery,
  UnloadableSchedulesQueryVariables,
} from './types/OverviewSchedulesRoot.types';
import {visibleRepoKeys} from './visibleRepoKeys';

export const OverviewSchedulesRoot = () => {
  useTrackPageView();
  useDocumentTitle('Overview | Schedules');

  const {allRepos, visibleRepos, loading: workspaceLoading} = React.useContext(WorkspaceContext);
  const repoCount = allRepos.length;
  const [searchValue, setSearchValue] = useQueryPersistedState<string>({
    queryKey: 'search',
    defaults: {search: ''},
  });

  const codeLocationFilter = useCodeLocationFilter();
  const runningStateFilter = useInstigationStatusFilter();

  const filters = React.useMemo(
    () => [codeLocationFilter, runningStateFilter],
    [codeLocationFilter, runningStateFilter],
  );
  const {button: filterButton, activeFiltersJsx} = useFilters({filters});

  const queryResultOverview = useQuery<OverviewSchedulesQuery, OverviewSchedulesQueryVariables>(
    OVERVIEW_SCHEDULES_QUERY,
    {
      fetchPolicy: 'network-only',
      notifyOnNetworkStatusChange: true,
    },
  );
  const {data, loading} = queryResultOverview;

  const refreshState = useQueryRefreshAtInterval(queryResultOverview, FIFTEEN_SECONDS);

  const repoBuckets = React.useMemo(() => {
    const visibleKeys = visibleRepoKeys(visibleRepos);
    return buildBuckets(data).filter(({repoAddress}) =>
      visibleKeys.has(repoAddressAsHumanString(repoAddress)),
    );
  }, [data, visibleRepos]);

  const {state: runningState} = runningStateFilter;
  const filteredBuckets = React.useMemo(() => {
    return repoBuckets.map(({schedules, ...rest}) => {
      return {
        ...rest,
        schedules: runningState.size
          ? schedules.filter(({scheduleState}) => runningState.has(scheduleState.status))
          : schedules,
      };
    });
  }, [repoBuckets, runningState]);

  const sanitizedSearch = searchValue.trim().toLocaleLowerCase();
  const anySearch = sanitizedSearch.length > 0;

  const filteredBySearch = React.useMemo(() => {
    const searchToLower = sanitizedSearch.toLocaleLowerCase();
    return filteredBuckets
      .map(({repoAddress, schedules}) => ({
        repoAddress,
        schedules: schedules.filter(({name}) => name.toLocaleLowerCase().includes(searchToLower)),
      }))
      .filter(({schedules}) => schedules.length > 0);
  }, [filteredBuckets, sanitizedSearch]);

  const anySchedulesVisible = React.useMemo(
    () => filteredBySearch.some(({schedules}) => schedules.length > 0),
    [filteredBySearch],
  );

  // Collect all schedules across visible code locations that the viewer has permission
  // to start or stop.
  const allPermissionedSchedules = React.useMemo(() => {
    return repoBuckets
      .map(({repoAddress, schedules}) => {
        return schedules
          .filter(({scheduleState}) => filterPermissionedInstigationState(scheduleState))
          .map(({name, scheduleState}) => ({
            repoAddress,
            scheduleName: name,
            scheduleState,
          }));
      })
      .flat();
  }, [repoBuckets]);

  // Build a list of keys from the permissioned schedules for use in checkbox state.
  // This includes collapsed code locations.
  const allPermissionedScheduleKeys = React.useMemo(() => {
    return allPermissionedSchedules.map(({repoAddress, scheduleName}) =>
      makeScheduleKey(repoAddress, scheduleName),
    );
  }, [allPermissionedSchedules]);

  const [{checkedIds: checkedKeys}, {onToggleFactory, onToggleAll}] = useSelectionReducer(
    allPermissionedScheduleKeys,
  );

  // Filter to find keys that are visible given any text search.
  const permissionedKeysOnScreen = React.useMemo(() => {
    const filteredKeys = new Set(
      filteredBySearch
        .map(({repoAddress, schedules}) => {
          return schedules.map(({name}) => makeScheduleKey(repoAddress, name));
        })
        .flat(),
    );
    return allPermissionedScheduleKeys.filter((key) => filteredKeys.has(key));
  }, [allPermissionedScheduleKeys, filteredBySearch]);

  // Determine the list of schedule objects that have been checked by the viewer.
  // These are the schedules that will be operated on by the bulk start/stop action.
  const checkedSchedules = React.useMemo(() => {
    const checkedKeysOnScreen = new Set(
      permissionedKeysOnScreen.filter((key: string) => checkedKeys.has(key)),
    );
    return allPermissionedSchedules.filter(({repoAddress, scheduleName}) => {
      return checkedKeysOnScreen.has(makeScheduleKey(repoAddress, scheduleName));
    });
  }, [permissionedKeysOnScreen, allPermissionedSchedules, checkedKeys]);

  const viewerHasAnyInstigationPermission = allPermissionedScheduleKeys.length > 0;
  const checkedCount = checkedSchedules.length;

  const content = () => {
    if (loading && !data) {
      return (
        <Box flex={{direction: 'row', justifyContent: 'center'}} style={{paddingTop: '100px'}}>
          <Box flex={{direction: 'row', alignItems: 'center', gap: 16}}>
            <Spinner purpose="body-text" />
            <div style={{color: colorTextLight()}}>Loading schedules…</div>
          </Box>
        </Box>
      );
    }

    const anyReposHidden = allRepos.length > visibleRepos.length;

    if (!filteredBySearch.length) {
      if (anySearch) {
        return (
          <Box padding={{top: 20}}>
            <NonIdealState
              icon="search"
              title="No matching schedules"
              description={
                anyReposHidden ? (
                  <div>
                    No schedules matching <strong>{searchValue}</strong> were found in the selected
                    code locations
                  </div>
                ) : (
                  <div>
                    No schedules matching <strong>{searchValue}</strong> were found in your
                    definitions
                  </div>
                )
              }
            />
          </Box>
        );
      }

      return (
        <Box padding={{top: 20}}>
          <NonIdealState
            icon="search"
            title="No schedules"
            description={
              anyReposHidden
                ? 'No matching schedules were found in the selected code locations'
                : 'No matching schedules were found in your definitions'
            }
          />
        </Box>
      );
    }

    return (
      <OverviewScheduleTable
        headerCheckbox={
          viewerHasAnyInstigationPermission ? (
            <CheckAllBox
              checkedCount={checkedCount}
              totalCount={permissionedKeysOnScreen.length}
              onToggleAll={onToggleAll}
            />
          ) : undefined
        }
        repos={filteredBySearch}
        checkedKeys={checkedKeys}
        onToggleCheckFactory={onToggleFactory}
      />
    );
  };

  const showSearchSpinner = (workspaceLoading && !repoCount) || (loading && !data);

  return (
    <Box flex={{direction: 'column'}} style={{height: '100%', overflow: 'hidden'}}>
      <PageHeader
        title={<Heading>Overview</Heading>}
        tabs={<OverviewTabs tab="schedules" refreshState={refreshState} />}
      />
      <Box
        padding={{horizontal: 24, vertical: 16}}
        flex={{direction: 'row', alignItems: 'center', justifyContent: 'space-between'}}
      >
        <Box flex={{direction: 'row', gap: 12}}>
          {filterButton}
          <TextInput
            icon="search"
            value={searchValue}
            rightElement={
              showSearchSpinner ? (
                <SearchInputSpinner tooltipContent="Loading schedules…" />
              ) : undefined
            }
            onChange={(e) => {
              setSearchValue(e.target.value);
              onToggleAll(false);
            }}
            placeholder="Filter by schedule name…"
            style={{width: '340px'}}
          />
        </Box>
        <Tooltip
          content="You do not have permission to start or stop these schedules"
          canShow={anySchedulesVisible && !viewerHasAnyInstigationPermission}
          placement="top-end"
          useDisabledButtonTooltipFix
        >
          <ScheduleBulkActionMenu
            schedules={checkedSchedules}
            onDone={() => refreshState.refetch()}
          />
        </Tooltip>
      </Box>
      {activeFiltersJsx.length ? (
        <Box
          padding={{vertical: 8, horizontal: 24}}
          border="top-and-bottom"
          flex={{direction: 'row', gap: 8}}
        >
          {activeFiltersJsx}
        </Box>
      ) : null}
      {loading && !repoCount ? (
        <Box padding={64}>
          <Spinner purpose="page" />
        </Box>
      ) : (
        <>
          {data?.unloadableInstigationStatesOrError.__typename === 'InstigationStates' ? (
            <UnloadableSchedulesAlert
              count={data.unloadableInstigationStatesOrError.results.length}
            />
          ) : null}
          <SchedulerInfo
            daemonHealth={data?.instance.daemonHealth}
            padding={{vertical: 16, horizontal: 24}}
            border="top"
          />
          {content()}
        </>
      )}
    </Box>
  );
};

const UnloadableSchedulesAlert = ({count}: {count: number}) => {
  const [isOpen, setIsOpen] = React.useState(false);

  if (!count) {
    return null;
  }

  const title = count === 1 ? '1 unloadable schedule' : `${count} unloadable schedules`;

  return (
    <>
      <Box padding={{vertical: 16, horizontal: 24}} border="top">
        <Alert
          intent="warning"
          title={title}
          description={
            <Box flex={{direction: 'column', gap: 12, alignItems: 'flex-start'}}>
              <div>
                Schedules were previously started but now cannot be loaded. They may be part of a
                code locations that no longer exist. You can turn them off, but you cannot turn them
                back on.
              </div>
              <Button onClick={() => setIsOpen(true)}>
                {count === 1 ? 'View unloadable schedule' : 'View unloadable schedules'}
              </Button>
            </Box>
          }
        />
      </Box>
      <Dialog
        isOpen={isOpen}
        title="Unloadable schedules"
        style={{width: '90vw', maxWidth: '1200px'}}
      >
        <Box padding={{bottom: 8}}>
          <UnloadableScheduleDialog />
        </Box>
        <DialogFooter>
          <Button intent="primary" onClick={() => setIsOpen(false)}>
            Done
          </Button>
        </DialogFooter>
      </Dialog>
    </>
  );
};

const UnloadableScheduleDialog = () => {
  const {data} = useQuery<UnloadableSchedulesQuery, UnloadableSchedulesQueryVariables>(
    UNLOADABLE_SCHEDULES_QUERY,
  );
  if (!data) {
    return <Spinner purpose="section" />;
  }

  if (data?.unloadableInstigationStatesOrError.__typename === 'InstigationStates') {
    return (
      <UnloadableSchedules
        scheduleStates={data.unloadableInstigationStatesOrError.results}
        showSubheading={false}
      />
    );
  }

  return <PythonErrorInfo error={data?.unloadableInstigationStatesOrError} />;
};

type RepoBucket = {
  repoAddress: RepoAddress;
  schedules: {name: string; scheduleState: BasicInstigationStateFragment}[];
};

const buildBuckets = (data?: OverviewSchedulesQuery): RepoBucket[] => {
  if (data?.workspaceOrError.__typename !== 'Workspace') {
    return [];
  }

  const entries = data.workspaceOrError.locationEntries.map((entry) => entry.locationOrLoadError);

  const buckets = [];

  for (const entry of entries) {
    if (entry?.__typename !== 'RepositoryLocation') {
      continue;
    }

    for (const repo of entry.repositories) {
      const {name, schedules} = repo;
      const repoAddress = buildRepoAddress(name, entry.name);
      const scheduleNames = schedules.map(({name, scheduleState}) => ({name, scheduleState}));

      if (scheduleNames.length > 0) {
        buckets.push({
          repoAddress,
          schedules: scheduleNames,
        });
      }
    }
  }

  return sortRepoBuckets(buckets);
};

const OVERVIEW_SCHEDULES_QUERY = gql`
  query OverviewSchedulesQuery {
    workspaceOrError {
      ... on Workspace {
        id
        locationEntries {
          id
          locationOrLoadError {
            ... on RepositoryLocation {
              id
              name
              repositories {
                id
                name
                schedules {
                  id
                  name
                  description
                  scheduleState {
                    id
                    ...BasicInstigationStateFragment
                  }
                }
              }
            }
            ...PythonErrorFragment
          }
        }
      }
      ...PythonErrorFragment
    }
    unloadableInstigationStatesOrError(instigationType: SCHEDULE) {
      ... on InstigationStates {
        results {
          id
        }
      }
    }
    instance {
      id
      ...InstanceHealthFragment
    }
  }

  ${BASIC_INSTIGATION_STATE_FRAGMENT}
  ${PYTHON_ERROR_FRAGMENT}
  ${INSTANCE_HEALTH_FRAGMENT}
`;

const UNLOADABLE_SCHEDULES_QUERY = gql`
  query UnloadableSchedulesQuery {
    unloadableInstigationStatesOrError(instigationType: SCHEDULE) {
      ... on InstigationStates {
        results {
          id
          ...InstigationStateFragment
        }
      }
      ...PythonErrorFragment
    }
  }

  ${INSTIGATION_STATE_FRAGMENT}
  ${PYTHON_ERROR_FRAGMENT}
`;
