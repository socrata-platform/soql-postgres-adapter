// Set up the libraries
@Library('socrata-pipeline-library')

// set up service and project variables
def service_server = 'soql-server-pg'
def service_secondary = 'secondary-watcher-pg'
def project_wd_server = 'soql-server-pg'
def project_wd_secondary = 'store-pg'

def isPr = env.CHANGE_ID != null;

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service_server, '.', [project_wd_server, project_wd_secondary])
def dockerize_server = new com.socrata.Dockerize(steps, service_server, BUILD_NUMBER)
def dockerize_secondary = new com.socrata.Dockerize(steps, service_secondary, BUILD_NUMBER)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build without creating a new tag.')
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
  }
  agent {
    label params.AGENT
  }
  environment {
    SERVICE = 'soql-server-pg'
    DEPLOY_PATTERN = 'soql-server-pg*'
    SECONDARY_DEPLOY_PATTERN = 'secondary-watcher-pg*'
  }
  stages {
    stage('Release Tag') {
      when {
        expression { return params.RELEASE_BUILD }
      }
      steps {
        script {
          if (params.RELEASE_DRY_RUN) {
            echo 'DRY RUN: Skipping release tag creation'
          }
          else {
            // get a list of all files changes since the last tag
            files = sh(returnStdout: true, script: "git diff --name-only HEAD `git describe --match \"v*\" --abbrev=0`").trim()
            echo "Files changed:\n${files}"

            // the release build process changes the version file, so it will always be changed
            // if there are other files changed, then increment the version, create a new tag and publish the changes
            if (files != 'version.sbt') {
              publishStage = true

              echo 'Running sbt-release'

              // The git config setup required for your project prior to running 'sbt release with-defaults' may vary:
// EN-59946 commenting out because I don't think this is necessary -- other similar jobs do not have this.  I will follow up after the release build is run to remove or uncomment.
//              sh(returnStdout: true, script: "git config user.name \'Jenkins Server in aws-us-west-2-infrastructure\'")
//              sh(returnStdout: true, script: "git config user.email \'test-infrastructure-l@socrata.com\'")
//              sh(returnStdout: true, script: "#!/bin/sh -e\ngit config remote.origin.url \"https://${GITHUB_API_TOKEN}@github.com/socrata-platform/soql-postgres-adapter.git\"")
              sh(returnStdout: true, script: "git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*")
              sh(returnStdout: true, script: "git config branch.main.remote origin")
              sh(returnStdout: true, script: "git config branch.main.merge refs/heads/main")

              echo sh(returnStdout: true, script: "echo y | sbt \"release with-defaults\"")
            }
          }
          echo 'Getting release tag'
          release_tag = sh(returnStdout: true, script: "git describe --abbrev=0 --match \"v*\"").trim()
          branchSpecifier = "refs/tags/${release_tag}"
          echo branchSpecifier

          // checkout the tag so we're performing subsequent actions on it
          sh "git checkout ${branchSpecifier}"
        }
      }
    }
    stage('Build Server') {
      steps {
        script {
          echo "Building sbt project..."
          sbtbuild.setScalaVersion("2.12")
          sbtbuild.build()

          env.SERVICE_VERSION = sbtbuild.getServiceVersion()
          // set the SERVICE_SHA to the current head because it might not be the same as env.GIT_COMMIT
          env.SERVICE_SHA = sh(returnStdout: true, script: "git rev-parse HEAD").trim()
          // set build description to be the same as the docker deploy tag
          currentBuild.description = "${env.SERVICE}:${env.SERVICE_VERSION}_${env.BUILD_NUMBER}_${env.SERVICE_SHA.take(8)}"
        }
      }
    }
    stage('Dockerize Server') {
      when {
        not { expression { isPr } }
      }
      steps {
        script {
          dockerize_server.docker_build(sbtbuild.getServiceVersion(), service_sha, sbtbuild.getDockerPath(project_wd_server), sbtbuild.getDockerArtifact(project_wd_server))
          env.DOCKER_TAG = dockerize_server.getDeployTag()
        }
      }
      post {
        success {
          script {
            if (params.RELEASE_BUILD){
              echo env.DOCKER_TAG // For now, just print the deploy tag in the console output -- later, communicate to release metadata service
            }
          }
        }
      }
    }
    stage('Dockerize Secondry') {
      when {
        not { expression { isPr } }
      }
      steps {
        script {
          dockerize_secondary.docker_build(sbtbuild.getServiceVersion(), service_sha, sbtbuild.getDockerPath(project_wd_secondary), sbtbuild.getDockerArtifact(project_wd_secondary))
          env.SECONDARY_DOCKER_TAG = dockerize_secondary.getDeployTag()
        }
      }
      post {
        success {
          script {
            if (params.RELEASE_BUILD){
              echo env.SECONDARY_DOCKER_TAG // For now, just print the deploy tag in the console output -- later, communicate to release metadata service
            }
          }
        }
      }
    }
    stage('Deploy') {
      when {
        not { expression { isPr } }
        not { expression { return params.RELEASE_BUILD } }
      }
      steps {
        script {
          // uses env.DOCKER_TAG and deploys to staging by default
          marathonDeploy(serviceName: env.DEPLOY_PATTERN, waitTime: '30')
          // deploys to staging by default
          marathonDeploy(serviceName: env.SECONDARY_DEPLOY_PATTERN, tag: SECONDARY_DOCKER_TAG, waitTime: '30')
        }
      }
    }
    stage('Deploy PG Control Mirrors') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { return params.RELEASE_BUILD} }
        }
      }
      steps {
        script {
          // uses env.DOCKER_TAG and deploys to staging by default
          marathonDeploy(serviceName: 'soql-server-mirror-control-pg1-staging', waitTime: '30')
          // deploys to staging by default
          marathonDeploy(serviceName: 'secondary-watcher-mirror-control-pg*', tag: SECONDARY_DOCKER_TAG, waitTime: '30')
        }
      }
    }
    stage('Deploy Citus Mirrors') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { return params.RELEASE_BUILD} }
        }
      }
      steps {
        script {
          // uses env.DOCKER_TAG and deploys to staging by default
          marathonDeploy(serviceName: 'soql-server-mirror-citus1-staging', waitTime: '30')
          // deploys to staging by default
          marathonDeploy(serviceName: 'secondary-watcher-mirror-citus*', tag: SECONDARY_DOCKER_TAG, waitTime: '30')
        }
      }
    }
  }
}
