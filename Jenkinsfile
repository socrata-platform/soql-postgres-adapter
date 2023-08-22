// Set up the libraries
@Library('socrata-pipeline-library')

// set up service and project variables
def service_server = 'soql-server-pg'
def project_wd_server = 'soql-server-pg'
def service_secondary = 'secondary-watcher-pg'
def project_wd_secondary = 'store-pg'

def isPr = env.CHANGE_ID != null;

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service_server, '.', [project_wd_server, project_wd_secondary])
def dockerize_server = new com.socrata.Dockerize(steps, service_server, BUILD_NUMBER)
def dockerize_secondary = new com.socrata.Dockerize(steps, service_secondary, BUILD_NUMBER)
def releaseTag = new com.socrata.ReleaseTag(steps, service_server)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build without creating a new tag.')
    string(name: 'RELEASE_NAME', defaultValue: '', description: 'For release builds, the release name which is used for the git tag and the deploy tag.')
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
  }
  agent {
    label params.AGENT
  }
  environment {
    SERVICE = "${service_server}"
    DEPLOY_PATTERN = "${service_server}*"
    SECONDARY_DEPLOY_PATTERN = "${service_secondary}*"
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
            releaseTag.create(params.RELEASE_NAME)
          }
        }
      }
    }
    stage('Build SoQL Server PG') {
      steps {
        script {
          echo "Building sbt project..."
          sbtbuild.setScalaVersion("2.12")
          // This build is a little unusual; it actually has _two_ artifacts, this one
          // and one for store-pg-assembly.  sbtbuild isn't really set up to override names
          // for two separate artifacts, so here we override the primary, and we'll override
          // the secondary down below when it's passed to dockerize.
          sbtbuild.setSrcJar("soql-server-pg/target/soql-server-pg-assembly.jar")
          env.STORE_PG_ARTIFACT = "store-pg/target/store-pg-assembly.jar"
          sbtbuild.build()
        }
      }
    }
    stage('Dockerize SoQL Server PG') {
      when {
        not { expression { isPr } }
      }
      steps {
        script {
          if (params.RELEASE_BUILD) {
            env.REGISTRY_PUSH = (params.RELEASE_DRY_RUN) ? 'none' : 'all'
            env.DOCKER_TAG = dockerize_server.docker_build_specify_tag_and_push(params.RELEASE_NAME, sbtbuild.getDockerPath(project_wd_server), sbtbuild.getDockerArtifact(project_wd_server), env.REGISTRY_PUSH)
          } else {
            env.REGISTRY_PUSH = 'internal'
            env.DOCKER_TAG = dockerize_server.docker_build('STAGING', env.GIT_COMMIT, sbtbuild.getDockerPath(project_wd_server), sbtbuild.getDockerArtifact(project_wd_server), env.REGISTRY_PUSH)
          }
        }
      }
      post {
        success {
          script {
            if (params.RELEASE_BUILD && !params.RELEASE_DRY_RUN) {
              echo env.DOCKER_TAG // For now, just print the deploy tag in the console output -- later, communicate to release metadata service
            }
          }
        }
      }
    }
    stage('Dockerize Secondary') {
      when {
        not { expression { isPr } }
      }
      steps {
        script {
          // Here's where we're getting the secondary artifact (named
          // via env.STORE_PG_ARTIFACT) out for dockerizing, per the
          // comment above.
          if (params.RELEASE_BUILD) {
            env.REGISTRY_PUSH = (params.RELEASE_DRY_RUN) ? 'none' : 'all'
            env.SECONDARY_DOCKER_TAG = dockerize_secondary.docker_build_specify_tag_and_push(params.RELEASE_NAME, sbtbuild.getDockerPath(project_wd_secondary), env.STORE_PG_ARTIFACT, env.REGISTRY_PUSH)
          } else {
            env.REGISTRY_PUSH = 'internal'
            env.SECONDARY_DOCKER_TAG = dockerize_secondary.docker_build('STAGING', env.GIT_COMMIT, sbtbuild.getDockerPath(project_wd_secondary), env.STORE_PG_ARTIFACT, env.REGISTRY_PUSH)
          }
          currentBuild.description = "${env.DOCKER_TAG} & ${env.SECONDARY_DOCKER_TAG}"
        }
      }
      post {
        success {
          script {
            if (params.RELEASE_BUILD && !params.RELEASE_DRY_RUN) {
              echo env.SECONDARY_DOCKER_TAG // For now, just print the deploy tag in the console output -- later, communicate to release metadata service
            }
          }
        }
      }
    }
    stage('Deploys:') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { return params.RELEASE_BUILD} }
        }
      }
      stages {
        stage('Deploy SoQL Server PG') {
          steps {
            script {
              // uses env.DOCKER_TAG and deploys to staging by default
              marathonDeploy(serviceName: env.DEPLOY_PATTERN, waitTime: '60')
            }
          }
        }
        stage('Deploy Secondary') {
          steps {
            script {
              // deploys to staging by default
              marathonDeploy(serviceName: env.SECONDARY_DEPLOY_PATTERN, tag: env.SECONDARY_DOCKER_TAG)
            }
          }
        }
      }
    }
  }
}
