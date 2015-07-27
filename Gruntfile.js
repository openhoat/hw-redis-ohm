'use strict';

module.exports = function (grunt) {
  var gruntConfig;
  gruntConfig = {
    clean: {
      default: ['dist']
    },
    mkdir: {
      all: {
        options: {
          create: ['dist/reports']
        }
      }
    },
    jshint: {
      options: {
        reporter: require('jshint-stylish'),
        force: true,
        jshintrc: 'jshint.json'
      },
      src: [
        'lib/**/*.js',
        'test/*.js',
        '*.js'
      ]
    },
    mochacov: {
      test: {
        options: {
          reporter: 'spec',
          growl: process.stdout.isTTY
        }
      },
      coverage: {
        options: {
          reporter: 'html-cov',
          output: 'dist/reports/coverage.html',
          coverage: true
        }
      },
      coveralls: {
        options: {
          coveralls: true,
          output: 'dist/reports/coverage.lcov'
        }
      },
      options: {
        files: ['test/*Spec.js']
      }
    }
  };
  if (process.env['JENKINS_URL']) { // If build is done into a Jenkins instance, set the config to make convenient reports (not for human)
    gruntConfig.jshint.options.reporter = 'checkstyle'; // Generate quality report with checkstyle format
    gruntConfig.jshint.options.reporterOutput = 'dist/reports/jshint_checkstyle.xml'; // Report file that Jenkins will parse
    gruntConfig.mochacov.test.options.reporter = 'xunit'; // Test report format is xunit (supported by Jenkins)
    gruntConfig.mochacov.test.options.output = 'dist/reports/test.xml';
    gruntConfig.mochacov.test.options.quiet = false;
    gruntConfig.mochacov.coverage.options.reporter = 'mocha-cobertura-reporter';
    gruntConfig.mochacov.coverage.options.output = 'dist/reports/coverage.xml';
    gruntConfig.mochacov.coverage.options.quiet = false;
    process.env['XUNIT_FILE'] = gruntConfig.mochacov.test.options.output; // Test report file that Jenkins will parse
  }
  grunt.registerTask('cleanXunitFile', 'Remove logs from xunit file', function () {
    var testFile, coverageFile;
    if (gruntConfig.mochacov.test.options.output && grunt.file.exists(gruntConfig.mochacov.test.options.output)) {
      testFile = grunt.file.read(gruntConfig.mochacov.test.options.output);
      if (testFile.indexOf('<testsuite')) {
        grunt.file.write(gruntConfig.mochacov.test.options.output, testFile.substring(testFile.indexOf('<testsuite')));
      }
    }
    if (gruntConfig.mochacov.coverage.options.output && grunt.file.exists(gruntConfig.mochacov.coverage.options.output)) {
      coverageFile = grunt.file.read(gruntConfig.mochacov.coverage.options.output);
      if (coverageFile.indexOf('<coverage')) {
        grunt.file.write(gruntConfig.mochacov.coverage.options.output, coverageFile.substring(coverageFile.indexOf('<coverage')));
      }
    }
  });
  require('load-grunt-tasks')(grunt);
  require('time-grunt')(grunt);
  grunt.initConfig(gruntConfig);
  grunt.registerTask('verify', ['mkdir', 'jshint']);
  grunt.registerTask('test', ['mkdir', 'mochacov:test', 'cleanXunitFile']);
  grunt.registerTask('coverage', ['mkdir', 'mochacov:test', 'mochacov:coverage', 'cleanXunitFile']);
  grunt.registerTask('coveralls', ['mkdir', 'mochacov:test', 'mochacov:coveralls', 'cleanXunitFile']);
  grunt.registerTask('default', ['verify', 'coverage']);
};