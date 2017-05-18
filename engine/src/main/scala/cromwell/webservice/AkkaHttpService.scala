package cromwell.webservice

import java.util.UUID

import akka.actor.{ActorRef, ActorRefFactory}
import akka.http.scaladsl.server.Directives._

import scala.concurrent.{ExecutionContext, Future}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Multipart.BodyPart
import akka.stream.ActorMaterializer
import cromwell.engine.backend.BackendConfiguration
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import cromwell.core.{WorkflowAborted, WorkflowId, WorkflowSubmitted}
import cromwell.core.Dispatcher.ApiDispatcher
import cromwell.engine.workflow.workflowstore.{WorkflowStoreActor, WorkflowStoreEngineActor, WorkflowStoreSubmitActor}
import akka.pattern.ask
import akka.util.{ByteString, Timeout}
import cromwell.engine.workflow.WorkflowManagerActor
import cromwell.services.metadata.MetadataService._
import cromwell.webservice.metadata.{MetadataBuilderActor, WorkflowQueryPagination}
import cromwell.webservice.metadata.MetadataBuilderActor.{BuiltMetadataResponse, FailedMetadataResponse, MetadataBuilderActorResponse}
import WorkflowJsonSupport._
import akka.http.scaladsl.coding.{Deflate, Gzip, NoCoding}
import akka.http.scaladsl.server.Route
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import com.typesafe.config.{Config, ConfigFactory}
import cromwell.engine.workflow.WorkflowManagerActor.WorkflowNotFoundException
import cromwell.engine.workflow.lifecycle.execution.callcaching.CallCacheDiffActor.{BuiltCallCacheDiffResponse, CallCacheDiffActorResponse, FailedCallCacheDiffResponse}
import cromwell.engine.workflow.lifecycle.execution.callcaching.{CallCacheDiffActor, CallCacheDiffQueryParameter}
import cromwell.engine.workflow.workflowstore.WorkflowStoreEngineActor.WorkflowStoreEngineActorResponse
import lenthall.exception.AggregatedMessageException

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

// FIXME: rename once the cutover happens
trait AkkaHttpService {
  // FIXME: check HTTP codes both before/after
  import cromwell.webservice.AkkaHttpService._

  implicit def actorRefFactory: ActorRefFactory
  implicit val materializer: ActorMaterializer
  implicit val ec: ExecutionContext

  val workflowStoreActor: ActorRef
  val workflowManagerActor: ActorRef
  val serviceRegistryActor: ActorRef

  // FIXME: make this bigger and elsewhere?
  val duration = 5.seconds
  implicit val timeout: Timeout = duration

  private val backendResponse = BackendResponse(BackendConfiguration.AllBackendEntries.map(_.name).sorted, BackendConfiguration.DefaultBackendEntry.name)

  // FIXME: Missing ruchi's label stuff
  val routes =
    path("workflows" / Segment / "backends") { version =>
      get { complete(backendResponse) }
    } ~
    path("engine" / Segment / "stats") { version =>
      get {
        onComplete(workflowManagerActor.ask(WorkflowManagerActor.EngineStatsCommand).mapTo[EngineStatsActor.EngineStats]) {
          case Success(stats) => complete(stats)
          case Failure(blah) =>
            val e = new RuntimeException("Unable to gather engine stats")
            e.failRequest(StatusCodes.InternalServerError)
        }
      }
    } ~
    path("engine" / Segment / "version") { version =>
      get { complete(versionResponse(ConfigFactory.load("cromwell-version.conf").getConfig("version"))) }
    } ~
    path("workflows" / Segment / Segment / "status") { (version, possibleWorkflowId) =>
      get { metadataBuilderRequest(possibleWorkflowId, (w: WorkflowId) => GetStatus(w)) }
    } ~
    path("workflows" / Segment / Segment / "outputs") { (version, possibleWorkflowId) =>
      get { metadataBuilderRequest(possibleWorkflowId, (w: WorkflowId) => WorkflowOutputs(w)) }
    } ~
    path("workflows" / Segment / Segment / "logs") { (version, possibleWorkflowId) =>
      get { metadataBuilderRequest(possibleWorkflowId, (w: WorkflowId) => GetLogs(w)) }
    } ~
    path("workflows" / Segment / "query") { version =>
      get {
        parameterSeq { parameters =>
          extractUri { uri =>
            metadataQueryRequest(parameters, uri)
          }
        }
      }
    } ~
    path("workflows" / Segment / "query") { version =>
      post {
        parameterSeq { parameters =>
          extractUri { uri =>
            metadataQueryRequest(parameters, uri)
          }
        }
      }
    } ~
    encodeResponseWith(Gzip, Deflate, NoCoding) {
      path("workflows" / Segment / Segment / "metadata") { (version, possibleWorkflowId) =>
        parameters('includeKey.*, 'excludeKey.*, 'expandSubWorkflows.as[Boolean].?) { (includeKeys, excludeKeys, expandSubWorkflowsOption) =>
          val includeKeysOption = NonEmptyList.fromList(includeKeys.toList)
          val excludeKeysOption = NonEmptyList.fromList(excludeKeys.toList)
          val expandSubWorkflows = expandSubWorkflowsOption.getOrElse(false)

          (includeKeysOption, excludeKeysOption) match {
            case (Some(_), Some(_)) =>
              val e = new IllegalArgumentException("includeKey and excludeKey may not be specified together")
              e.failRequest(StatusCodes.BadRequest)
            case (_, _) => metadataBuilderRequest(possibleWorkflowId, (w: WorkflowId) => GetSingleWorkflowMetadataAction(w, includeKeysOption, excludeKeysOption, expandSubWorkflows))
          }
        }
      }
    } ~
    path("workflows" / Segment / "callcaching" / "diff") { version =>
      parameterSeq { parameters =>
        get {
          CallCacheDiffQueryParameter.fromParameters(parameters) match {
            case Valid(queryParameter) =>
              val diffActor = actorRefFactory.actorOf(CallCacheDiffActor.props(serviceRegistryActor), "CallCacheDiffActor-" + UUID.randomUUID())
              onComplete(diffActor.ask(queryParameter).mapTo[CallCacheDiffActorResponse]) {
                case Success(r: BuiltCallCacheDiffResponse) => complete(r.response)
                case Success(r: FailedCallCacheDiffResponse) => r.reason.errorRequest(StatusCodes.InternalServerError)
                case Failure(e) => e.errorRequest(StatusCodes.InternalServerError)
              }
            case Invalid(errors) =>
              val e = AggregatedMessageException("Wrong parameters for call cache diff query", errors.toList)
              e.errorRequest(StatusCodes.BadRequest)
          }
        }
      }
    } ~
    path("workflows" / Segment / Segment / "timing") { (version, possibleWorkflowId) =>
      onComplete(validateWorkflowId(possibleWorkflowId)) {
        case Success(_) => getFromResource("workflowTimings/workflowTimings.html")
        case Failure(e) => e.failRequest(StatusCodes.InternalServerError)
      }
    } ~
    path("workflows" / Segment / Segment / "abort") { (version, possibleWorkflowId) =>
      post {
        val response = validateWorkflowId(possibleWorkflowId) flatMap { w =>
          workflowStoreActor.ask(WorkflowStoreActor.AbortWorkflow(w, workflowManagerActor)).mapTo[WorkflowStoreEngineActorResponse]
        }
        // FIXME: Clean up WorkflowStoreEngineActorResponse if there's some combination possible?
        // FIXME: and/or combine w/ the meatdata stuff
        onComplete(response) {
          case Success(WorkflowStoreEngineActor.WorkflowAborted(id)) => complete(WorkflowAbortResponse(id.toString, WorkflowAborted.toString))
          case Success(WorkflowStoreEngineActor.WorkflowAbortFailed(_, e: IllegalStateException)) => e.errorRequest(StatusCodes.Forbidden)
          case Success(WorkflowStoreEngineActor.WorkflowAbortFailed(_, e: WorkflowNotFoundException)) => e.errorRequest(StatusCodes.NotFound)
          case Success(WorkflowStoreEngineActor.WorkflowAbortFailed(_, e)) => e.errorRequest(StatusCodes.InternalServerError)
          case Failure(e: UnrecognizedWorkflowException) => e.failRequest(StatusCodes.NotFound)
          case Failure(e: InvalidWorkflowException) => e.failRequest(StatusCodes.BadRequest)
          // Something went awry with the actual request
          case Failure(e) => e.errorRequest(StatusCodes.InternalServerError)
          case (_) => complete("incomplete")
        }
      }
    } ~
    path("workflows" / Segment) { version =>
      post {
        entity(as[Multipart.FormData]) { formData =>
          submitRequest(formData, true)
        }
      }
    } ~
  path("workflows" / Segment / "batch") { version =>
    post {
      entity(as[Multipart.FormData]) { formData =>
        submitRequest(formData, false)
      }
    }
  }

  private def submitRequest(formData: Multipart.FormData, isSingleSubmission: Boolean): Route = {
    val allParts: Future[Map[String, ByteString]] = formData.parts.mapAsync[(String, ByteString)](1) {
      case b: BodyPart => b.toStrict(duration).map(strict => b.name -> strict.entity.data)
    }.runFold(Map.empty[String, ByteString])((map, tuple) => map + tuple)
    // FIXME: make this less hokey
    onComplete(allParts) {
      case Success(formData) =>
        PartialWorkflowSources.fromSubmitRoute(formData, allowNoInputs = isSingleSubmission) match {
          case Success(workflowSourceFiles) if workflowSourceFiles.size == 1 =>
            onComplete(workflowStoreActor.ask(WorkflowStoreActor.SubmitWorkflow(workflowSourceFiles.head)).mapTo[WorkflowStoreSubmitActor.WorkflowSubmittedToStore]) {
              case Success(w) => complete((StatusCodes.Created, WorkflowSubmitResponse(w.workflowId.toString, WorkflowSubmitted.toString)))
              case Failure(e) => e.failRequest(StatusCodes.InternalServerError)
            }
          case Success(workflowSourceFiles) if workflowSourceFiles.size == 1 =>
            onComplete(workflowStoreActor.ask(WorkflowStoreActor.SubmitWorkflow(workflowSourceFiles.head)).mapTo[WorkflowStoreSubmitActor.WorkflowSubmittedToStore]) {
              case Success(w) => complete((StatusCodes.Created, WorkflowSubmitResponse(w.workflowId.toString, WorkflowSubmitted.toString)))
              case Failure(e) => e.failRequest(StatusCodes.InternalServerError)
            }
          case Success(workflowSourceFiles) if isSingleSubmission =>
            val e = new IllegalArgumentException("To submit more than one workflow at a time, use the batch endpoint.")
            e.failRequest(StatusCodes.BadRequest)
          case Success(workflowSourceFiles) =>
            onComplete(workflowStoreActor.ask(WorkflowStoreActor.BatchSubmitWorkflows(NonEmptyList.fromListUnsafe(workflowSourceFiles.toList))).mapTo[WorkflowStoreSubmitActor.WorkflowsBatchSubmittedToStore]) {
              case Success(w) =>
                val responses = w.workflowIds map { id => WorkflowSubmitResponse(id.toString, WorkflowSubmitted.toString) }
                complete((StatusCodes.Created, responses.toList))
              case Failure(e) => e.failRequest(StatusCodes.InternalServerError)
            }
          case Failure(t) => t.failRequest(StatusCodes.BadRequest)
        }
      case Failure(e) => e.failRequest(StatusCodes.InternalServerError)
    }
  }

  private def validateWorkflowId(possibleWorkflowId: String): Future[WorkflowId] = {
    Try(WorkflowId.fromString(possibleWorkflowId)) match {
      case Success(w) =>
        serviceRegistryActor.ask(ValidateWorkflowId(w)).mapTo[WorkflowValidationResponse] map {
          case RecognizedWorkflowId => w
          case UnrecognizedWorkflowId => throw UnrecognizedWorkflowException(s"Unrecognized workflow ID: $w")
          case FailedToCheckWorkflowId(t) => throw t
        }
      case Failure(t) => Future.failed(InvalidWorkflowException(s"Invalid workflow ID: '$possibleWorkflowId'."))
    }
  }

  private def metadataBuilderRequest(possibleWorkflowId: String, request: WorkflowId => ReadAction): Route = {
    val metadataBuilderActor = actorRefFactory.actorOf(MetadataBuilderActor.props(serviceRegistryActor).withDispatcher(ApiDispatcher), MetadataBuilderActor.uniqueActorName)
    val response = validateWorkflowId(possibleWorkflowId) flatMap { w => metadataBuilderActor.ask(request(w)).mapTo[MetadataBuilderActorResponse] }

    onComplete(response) {
      case Success(r: BuiltMetadataResponse) => complete(r.response)
      case Success(r: FailedMetadataResponse) => r.reason.errorRequest(StatusCodes.InternalServerError)
      case Failure(e: UnrecognizedWorkflowException) => e.failRequest(StatusCodes.NotFound)
      case Failure(e: InvalidWorkflowException) => e.failRequest(StatusCodes.BadRequest)
      case Failure(e) => e.errorRequest(StatusCodes.InternalServerError)
    }
  }

  protected[this] def metadataQueryRequest(parameters: Seq[(String, String)], uri: Uri): Route = {
    val response = serviceRegistryActor.ask(WorkflowQuery(parameters)).mapTo[MetadataQueryResponse]

    onComplete(response) {
      case Success(w: WorkflowQuerySuccess) =>
        val headers = WorkflowQueryPagination.generateLinkHeaders(uri, w.meta)
        respondWithHeaders(headers) {
          complete(w.response)
        }
      case Success(w: WorkflowQueryFailure) => w.reason.failRequest(StatusCodes.BadRequest)
      case Failure(e) => e.errorRequest(StatusCodes.InternalServerError)
    }
  }
}

object AkkaHttpService {
  import spray.json._

  implicit class EnhancedThrowable(val e: Throwable) extends AnyVal {
    def failRequest(statusCode: StatusCode): Route = complete((statusCode, APIResponse.fail(e).toJson.prettyPrint))
    def errorRequest(statusCode: StatusCode): Route = complete((statusCode, APIResponse.error(e).toJson.prettyPrint))
  }

  final case class BackendResponse(supportedBackends: List[String], defaultBackend: String)

  final case class UnrecognizedWorkflowException(message: String) extends Exception(message)
  final case class InvalidWorkflowException(message: String) extends Exception(message)

  def versionResponse(versionConf: Config) = JsObject(Map("cromwell" -> versionConf.getString("cromwell").toJson))
}
