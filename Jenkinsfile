// Set up the libraries
@Library('socrata-pipeline-library@jon/better-subprojects')

// set up service and project variables
def service_server = "soql-server-pg"
def project_wd_server = "soql-server-pg"
//def project_name_server = "soqlServerPG"
def deploy_service_pattern_server = "soql-server-pg*"
def service_secondary = "secondary-watcher-pg"
def project_wd_secondary = "store-pg"
//def project_name_secondary = "storePG"
def deploy_service_pattern_secondary = "secondary-watcher-pg*"
def deploy_environment = "staging"
def default_branch_specifier = "origin/master"

def service_sha = env.GIT_COMMIT

// variables that determine which stages we run based on what triggered the job
def boolean stage_cut = false
def boolean stage_build = false
def boolean stage_dockerize = false
def boolean stage_deploy = false

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service_server, '.', [project_wd_server, project_wd_secondary])
//def build_server = new com.socrata.SBTBuild(steps, service_server, project_wd_server)
//build_server.setSubprojectName(project_name_server)
//build_server.setScalaVersion("2.12")
def dockerize_server = new com.socrata.Dockerize(steps, service_server, BUILD_NUMBER)
//def build_secondary = new com.socrata.SBTBuild(steps, service_secondary, project_wd_secondary)
//build_secondary.setSubprojectName(project_name_secondary)
//build_secondary.setScalaVersion("2.12")
def dockerize_secondary = new com.socrata.Dockerize(steps, service_secondary, BUILD_NUMBER)
def deploy = new com.socrata.MarathonDeploy(steps)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_CUT', defaultValue: false, description: 'Are we cutting a new release candidate?')
    booleanParam(name: 'FORCE_BUILD', defaultValue: false, description: 'Force build from latest tag if sbt release needed to be run between cuts')
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: default_branch_specifier, description: 'Use this branch for building the artifact.')
  }
  agent {
    label params.AGENT
  }
  environment {
    PATH = "${WORKER_PATH}"
  }

  stages {
    stage('Setup') {
      steps {
        script {
          // check to see if we want to use a non-standard branch and check out the repo
          if (params.BRANCH_SPECIFIER == default_branch_specifier) {
            checkout scm
          } else {
            def scmRepoUrl = scm.getUserRemoteConfigs()[0].getUrl()
            checkout ([
              $class: 'GitSCM',
              branches: [[name: params.BRANCH_SPECIFIER ]],
              userRemoteConfigs: [[ url: scmRepoUrl ]]
            ])
          }

          // set the service sha to what was checked out (GIT_COMMIT isn't always set)
          service_sha = sh(returnStdout: true, script: "git rev-parse HEAD").trim()

          // determine what triggered the build and what stages need to be run
          if (params.RELEASE_CUT == true) { // RELEASE_CUT parameter was set by a cut job
            stage_cut = true  // other stages will be turned on in the cut step as needed
            deploy_environment = "rc"
          }
          else if (env.CHANGE_ID != null) { // we're running a PR builder
            stage_build = true
          }
          else if (BRANCH_NAME == "master") { // we're running a build on master branch to deploy to staging
            stage_build = true
            stage_dockerize = true
            stage_deploy = true
          }
          else {
            // we're not sure what we're doing...
            echo "Unknown build trigger - Exiting as Failure"
            currentBuild.result = 'FAILURE'
            return
          }
        }
      }
    }
    stage('Cut') {
      when { expression { stage_cut } }
      steps {
        script {
          def cutNeeded = false

          // get a list of all files changes since the last tag
          files = sh(returnStdout: true, script: "git diff --name-only HEAD `git describe --match \"v*\" --abbrev=0`").trim()
          echo "Files changed:\n${files}"

          if (files == 'version.sbt') {
            // Build anyway using latest tag - needed if sbt release had to be run between cuts
            // This parameter will need to be set by the cut job in Jenkins
            if(params.FORCE_BUILD) {
              cutNeeded = true
            }
            else {
              echo "No build needed, skipping subsequent steps"
            }
          }
          else {
            echo 'Running sbt-release'

            // The git config setup required for your project prior to running 'sbt release with-defaults' may vary:
            sh(returnStdout: true, script: "git config user.name \'Jenkins Server in aws-us-west-2-infrastructure\'")
            sh(returnStdout: true, script: "git config user.email \'test-infrastructure-l@socrata.com\'")
            sh(returnStdout: true, script: "#!/bin/sh -e\ngit config remote.origin.url \"https://${GITHUB_API_TOKEN}@github.com/socrata-platform/soql-postgres-adapter.git\"")
            sh(returnStdout: true, script: "git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*")
            sh(returnStdout: true, script: "git config branch.master.remote origin")
            sh(returnStdout: true, script: "git config branch.master.merge refs/heads/master")

            echo sh(returnStdout: true, script: "echo y | sbt \"release with-defaults\"")

            cutNeeded = true
          }

          if(cutNeeded == true) {
            echo 'Getting release tag'
            release_tag = sh(returnStdout: true, script: "git describe --abbrev=0 --match \"v*\"").trim()
            branchSpecifier = "refs/tags/${release_tag}"
            echo branchSpecifier

            // checkout the tag so we're performing subsequent actions on it
            sh "git checkout ${branchSpecifier}"

            // set the service_sha to the current tag because it might not be the same as env.GIT_COMMIT
            service_sha = sh(returnStdout: true, script: "git rev-parse HEAD").trim()

            // set later stages to run since we're cutting
            stage_build = true
            stage_dockerize = true
            stage_deploy = true
          }
        }
      }
    }
    stage('Build Server') {
      when { expression { stage_build } }
      steps {
        script {
          echo "Building sbt project..."
          sbtbuild.setScalaVersion("2.12")
          sbtbuild.build()

          gonnafail = "not_a_real_subproject"
          echo "Test failing:  ${sbtbuild.getDockerArtifact(gonnafail)}"
        }
      }
    }
    stage('Dockerize Server') {
      when { expression { stage_dockerize } }
      steps {
        script {
          echo "Building docker container..."
          dockerize_server.docker_build(sbtbuild.getServiceVersion(), service_sha, sbtbuild.getDockerPath(project_wd_server), sbtbuild.getDockerArtifact(project_wd_server))
        }
      }
    }
    stage('Dockerize Secondry') {
      when { expression { stage_dockerize } }
      steps {
        script {
          echo "Building docker container..."
          dockerize_secondary.docker_build(sbtbuild.getServiceVersion(), service_sha, sbtbuild.getDockerPath(project_wd_secondary), sbtbuild.getDockerArtifact(project_wd_secondary))
        }
      }
    }
    stage('Deploy') {
      when { expression { stage_deploy } }
      steps {
        script {
          // Checkout and run bundle install in the apps-marathon repo
          deploy.checkoutAndInstall()

          // deploy the service to the specified environment
          deploy.deploy(deploy_service_pattern_server, deploy_environment, dockerize_server.getDeployTag())
          deploy.deploy(deploy_service_pattern_secondary, deploy_environment, dockerize_secondary.getDeployTag())
        }
      }
    }
  }
}
