'use strict';

const _ = require('lodash');
const fs = require('fs');
const path = require('path');
const Promise = require('bluebird');
const chalk = require('chalk');
const coveralls = require('gulp-coveralls');
const debug = require('gulp-debug');
const del = require('del');
const gulp = require('gulp');
const gulpif = require('gulp-if');
const gutil = require('gulp-util');
const istanbul = require('gulp-istanbul');
const eslint = require('gulp-eslint');
const licenseFinder = require('gulp-license-finder');
const minimist = require('minimist');
const mkdirp = require('mkdirp');
const mocha = require('gulp-mocha');
const notify = require('gulp-notify');
const f = (...args) => require('util').format(...args);
const pkg = require('./package');
const jenkins = !!process.env['JENKINS_URL'];

let taskSpecs;

Promise.promisifyAll(fs);
process.env['NODE_ENV'] = 'test';

const toRelativePath = (file, baseDir) => {
  if (Array.isArray(file)) {
    return file.map(file => toRelativePath(file));
  } else {
    return path.relative(baseDir || __dirname, file);
  }
};

const toArray = (s, sep, format) => s.split(sep || ',').map(item => f(format || '%s', item.trim()));

const rm = src => del([src], {dryRun: cmdOpt['dry-run']})
  .then(files => {
    if (cmdOpt.verbose) {
      gutil.log(files && files.length ? $.yellow(f('Files and folders deleted :', toRelativePath(files)
        .join(', '))) : $.yellow(f('Nothing deleted')));
    }
  });

/**
 * Command args
 */
const cmdOpt = minimist(process.argv.slice(2), {
  string: ['include', 'transaction', 'log-level'],
  boolean: ['log-body', 'dry-run', 'verbose', 'color'],
  default: {color: true},
  alias: {
    i: 'include',
    t: 'transaction',
    l: 'log-body',
    d: 'dry-run',
    v: 'verbose'
  }
});

if (cmdOpt['dry-run'] || cmdOpt['log-body']) {
  cmdOpt.verbose = true;
}
if (cmdOpt['log-level']) {
  process.env['HW_LOG_LEVEL'] = cmdOpt['log-level'];
}
const $ = new chalk.constructor({enabled: cmdOpt.color});
process.env['HW_LOG_COLORS'] = cmdOpt.color;

/**
 * Configuration
 */
const config = {
  distDir: 'dist',
  reportDir: 'dist/reports',
  lintReportDir: 'dist/reports/lint',
  testReportDir: 'dist/reports/test',
  files: {
    allSources: [
      '**',
      '!dist/**',
      '!node_modules/**',
      '!tmp/**'
    ],
    allJs: [
      '**/*.js',
      '!**/deprecated/**', '!**/*.deprecated.js',
      '!assets/**',
      '!dist/**',
      '!etc/**',
      '!node_modules/**',
      '!tmp/**'
    ]
  },
  test: {
    src: ['spec/*Spec.js'],
    options: {
      reporter: jenkins ? 'spec-xunit-file' : 'spec',
      grep: cmdOpt.transaction
    },
    coverage: {
      instrument: {
        pattern: ['lib/**/*.js']
      },
      reporters: ['text', 'html', 'lcov'],
      reportOpts: {
        html: {file: 'coverage.html'},
        lcov: {file: 'lcov.info'}
      }
    }
  }
};

_.merge(config, {
  lint: {
    src: config.files.allJs,
    options: {},
    format: jenkins ? 'checkstyle' : 'stylish',
    checkstyleReportFile: path.join(config.lintReportDir, 'checkstyle.xml')
  },
  test: {
    coverage: {
      reportOpts: {
        html: {
          dir: path.join(config.testReportDir, 'coverage/html')
        },
        lcov: {
          dir: path.join(config.testReportDir, 'coverage/lcov')
        }
      }
    },
    coveralls: {
      src: path.join(config.testReportDir, 'lcov.info')
    }
  }
});

if (jenkins) {
  process.env['XUNIT_FILE'] = path.join(config.testReportDir, 'xunit.xml');
  config.coverage.reporters.push('cobertura');
  config.coverage.reportOpts.cobertura = {
    dir: path.join(config.testReportDir, 'coverage/cobertura'),
    file: 'coverage.xml'
  };
}

taskSpecs = {
  default: {
    deps: 'help'
  },
  help: {
    desc: 'Show tasks descriptions',
    task: () => {
      let l = 0;
      const log = (...args) => {
        let newLine;
        if (typeof args[0] === 'boolean') {
          newLine = args[0];
          args.shift();
        }
        process.stdout.write(f(...args));
        if (newLine !== false) {
          process.stdout.write('\n');
        }
      };
      log();
      log($.bold('Usage'));
      log('  gulp %s', $.cyan('task'));
      log();
      log($.bold('Tasks'));
      const tasks = [];
      _.forIn(taskSpecs, (taskSpec, taskSpecName) => {
        const task = _.pick(taskSpec, ['name', 'desc', 'deps', 'config', 'providesFn']);
        l = Math.max(taskSpecName.length, l);
        task.name = taskSpecName;
        task.providesFn = typeof taskSpec.task === 'function';
        tasks.push(task);
      });
      tasks.forEach(task => {
        log(false, '  %s : %s', $.cyan(_.padEnd(task.name, l)), task.desc);
        if (task.deps) {
          log(false, ' %s', $.yellow(f('[%s]', (Array.isArray(task.deps) ? task.deps : [task.deps]).join(', '))));
        }
        log(false, ' ');
        if (task.config) {
          log(false, '%s', $.yellow.bold(f('\u2692 ')));
        }
        if (task.providesFn) {
          log(false, '%s', $.green(f('\u0192 ')));
        }
        log();
      });
      log();
    }
  },
  clean: {
    desc: 'Clean all generated files',
    config: {src: path.join(config.distDir, '**/*'), continueOnDryRun: true},
    task: t => rm(t.config.src)
  },
  mkdir: {
    desc: 'Create dir to generate build files',
    config: {
      src: [config.distDir, config.reportDir, config.lintReportDir, config.testReportDir]
    },
    task: t => Promise.each(t.config.src, dir => Promise.fromNode(mkdirp.bind(mkdirp, dir))
      .then(dir => {
        if (cmdOpt.verbose) {
          gutil.log(dir ? $.yellow(f('Directory created :', toRelativePath(dir))) : $.yellow(f('Nothing created')));
        }
      }))
  },
  lint: {
    desc: 'Detect errors and potential problems in code',
    deps: 'mkdir',
    config: {
      src: cmdOpt.include ? toArray(cmdOpt.include, ',') : config.lint.src
    },
    task: t => {
      const checkstyle = config.lint.format === 'checkstyle'
        , reportFile = checkstyle ? fs.createWriteStream(config.lint.checkstyleReportFile) : null;
      return gulp.src(t.config.src)
        .pipe(eslint(config.lint.options))
        .pipe(eslint.format())
        .pipe(gulpif(checkstyle, eslint.format(config.lint.format, reportFile)))
        .pipe(gulpif(!!config.notifier, notify(_.defaults({
          onLast: true,
          message: 'Lint done'
        }, config.notifier))));
    }
  },
  test: {
    desc: 'Run mocha specs',
    deps: 'mkdir',
    config: {
      src: cmdOpt.include ? toArray(cmdOpt.include, ',', 'spec/%sSpec.js') : config.test.src
    },
    task: t => gulp.src(t.config.src, {read: false})
      .pipe(mocha(config.test.options))
  },
  coverage: {
    default: {
      desc: 'Run istanbul test coverage',
      deps: ['/mkdir', 'prepare'],
      config: {src: cmdOpt.include ? toArray(cmdOpt.include, ',', 'spec/%sSpec.js') : config.test.src},
      task: t => gulp.src(t.config.src, {read: false})
        .pipe(mocha(config.test.options))
        .pipe(istanbul.writeReports({
          dir: config.testReportDir,
          reporters: config.test.coverage.reporters,
          reportOpts: _.get(config.test.coverage, 'reporterOpts')
        }))
        .pipe(gulp.dest(config.testReportDir))
    },
    prepare: {
      desc: 'Prepare for test coverage',
      config: {src: config.test.coverage.instrument.pattern},
      task: t => gulp.src(t.config.src)
        .pipe(istanbul(config.test.coverage.instrument.options))
        .pipe(istanbul.hookRequire())
    }
  },
  coveralls: {
    default: {
      desc: 'Submit code coverage to coveralls',
      deps: '../coverage',
      config: {src: config.test.coveralls.src},
      task: t => gulp.src(t.config.src)
        .pipe(coveralls())
    }
  },
  licenses: {
    desc: 'Find licenses in node project and dependencies',
    task: () => {
      const dest = path.join(config.distDir, 'licenses.csv');
      const lf = licenseFinder(path.basename(dest), {
        csv: true,
        depth: 1
      });
      lf.once('finish', () => {
        if (cmdOpt.verbose) {
          gutil.log($.yellow(f('Created license report : %s', dest)));
        }
        lf.emit('end');
      });
      lf.pipe(gulp.dest(path.dirname(dest)));
      return lf;
    }
  },
  sources: {
    desc: 'Get all source files',
    task: () => gulp.src(config.files.allSources)
      .pipe(debug({title: '', minimal: true}))
      .pipe(gulp.dest(config.sourcesListDir))
  },
  version: {
    desc: 'Display package version',
    task: (t, cb) => {
      console.log(f('%s/%s', pkg.name, pkg.version));
      cb();
    }
  }
};

const taskSpecTransformer = baseNs => (result, taskSpec, taskSpecName) => {
  const isTaskGroup = () => !Object.keys(_.pick(taskSpec, ['deps', 'task', 'desc'])).length;
  const dryRun = () => {
    if (cmdOpt['dry-run']) {
      if (_.get(item, 'config.src')) {
        return gulp.src(item.config.src)
          .pipe(debug({title: ns}));
      }
      return true;
    }
  };
  const ns = baseNs ?
    (
      taskSpecName === (_.get(config, 'taskSpecs.defaultGroupTask') || 'default') ?
        baseNs :
        path.join(baseNs, taskSpecName)
    ) :
    taskSpecName;
  const group = isTaskGroup();
  if (group) {
    _.transform(taskSpec, taskSpecTransformer(ns), result);
    return;
  }
  const item = result[ns] = _.omit(taskSpec, ['desc', 'deps', 'task', 'config']);
  if (taskSpec.desc) {
    item.desc = typeof taskSpec.desc === 'function' ? taskSpec.desc(taskSpecName, taskSpec) : taskSpec.desc;
  }
  if (taskSpec.deps) {
    item.deps = [];
    (Array.isArray(taskSpec.deps) ? taskSpec.deps : [taskSpec.deps]).forEach(dep => {
      if (dep.indexOf('/') === 0) {
        item.deps.push(dep.substring(1));
      } else {
        item.deps.push(baseNs ? path.join(baseNs, dep) : dep);
      }
    });
  }
  if (typeof taskSpec.task === 'function') {
    item.task = cb => {
      if (dryRun(item.config)) {
        if (!_.get(item, 'config.continueOnDryRun')) {
          return cb();
        }
      }
      return taskSpec.task(_.omit(item, 'task'), (err, data) => {
        if (cmdOpt.verbose && data) {
          gutil.log($.yellow('Task result :', data));
        }
        cb(err);
      });
    };
  }
  if (taskSpec.config) {
    item.config = taskSpec.config;
  }
};

taskSpecs = _.transform(taskSpecs, taskSpecTransformer(), {});

const registerTasks = taskSpecs => {
  _.forIn(taskSpecs, (taskSpec, taskSpecName) => {
    const args = [taskSpecName];
    if (!taskSpec.desc && taskSpec.deps && taskSpec.deps.length === 1) {
      taskSpec.desc = taskSpecs[_.first(taskSpec.deps)].desc;
    }
    if (taskSpec.deps) {
      args.push(taskSpec.deps);
    }
    if (taskSpec.task) {
      args.push(taskSpec.task);
    }
    gulp.task(...args);
  });
};
registerTasks(taskSpecs);
