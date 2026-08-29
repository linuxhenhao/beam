# Third-party terminal assets

The files under `vendor/` are prebuilt, unmodified distributions bundled into
the `beam-daemon` binary so the browser terminal works offline without a CDN
dependency or a Node/TypeScript build chain.

| File | Project | Version | License | SHA-256 |
| --- | --- | --- | --- | --- |
| `vendor/xterm.min.js` | [xterm.js](https://github.com/xtermjs/xterm.js) | 5.3.0 | MIT | `fc1dd31b221e3e5f929486e07a80b477a8aaf9dce2b4f9c3ffe7dd25f370655d` |
| `vendor/xterm.css` | [xterm.js](https://github.com/xtermjs/xterm.js) | 5.3.0 | MIT | `832f3f2c603b43ad4351ff04970150cc7a873014276db126a6065c6dd81e4872` |
| `vendor/xterm-addon-fit.min.js` | [xterm-addon-fit](https://github.com/xtermjs/xterm.js) | 0.8.0 | MIT | `c78e5795c6487acdf24ab436798d4a9cec3848101b281bc65b061f39db714be1` |
| `vendor/xterm-addon-web-links.min.js` | [xterm-addon-web-links](https://github.com/xtermjs/xterm.js) | 0.9.0 | MIT | `fe28b3cce677fc3460eb2ee2bb5d759ae2e75955bc6f30bd57afc400fbb484af` |

Versions were pinned on 2026-08-29. To refresh, download the same version
from a package registry, re-run `sha256sum`, and update this table.
