import {Icon, ExternalAnchorButton} from '@dagster-io/ui';
import * as React from 'react';

import {AppContext} from './AppContext';
import {CodeLinkProtocolContext} from './CodeLinkProtocol';

export const CodeLink: React.FC<{file: string; lineNumber: number}> = ({file, lineNumber}) => {
  const {codeLinksEnabled} = React.useContext(AppContext);
  const [codeLinkProtocol, _] = React.useContext(CodeLinkProtocolContext);

  if (!codeLinksEnabled) {
    return null;
  }

  const codeLink = codeLinkProtocol.protocol
    .replace('{FILE}', file)
    .replace('{LINE}', lineNumber.toString());
  return (
    <ExternalAnchorButton icon={<Icon name="open_in_new" />} href={codeLink}>
      Open in editor
    </ExternalAnchorButton>
  );
};

export const VersionControlCodeLink: React.FC<{
  versionControlUrl: string;
  pathInModule: string;
  lineNumber: number;
}> = ({versionControlUrl, pathInModule, lineNumber}) => {
  const codeLink = versionControlUrl + '/' + pathInModule + '#L' + lineNumber.toString();

  if (versionControlUrl.includes('github.com')) {
    return (
      <ExternalAnchorButton icon={<Icon name="github" />} href={codeLink}>
        Open in GitHub
      </ExternalAnchorButton>
    );
  } else if (versionControlUrl.includes('gitlab.com')) {
    return (
      <ExternalAnchorButton icon={<Icon name="gitlab" />} href={codeLink}>
        Open in GitLab
      </ExternalAnchorButton>
    );
  }
  return (
    <ExternalAnchorButton icon={<Icon name="open_in_new" />} href={codeLink}>
      Open source
    </ExternalAnchorButton>
  );
};
