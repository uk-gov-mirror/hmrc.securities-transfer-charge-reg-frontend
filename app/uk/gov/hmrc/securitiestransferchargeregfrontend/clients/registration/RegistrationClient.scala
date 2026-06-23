/*
 * Copyright 2025 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.hmrc.securitiestransferchargeregfrontend.clients.registration

import play.api.Logging
import play.api.http.Status.{BAD_REQUEST, CREATED, NOT_FOUND, NO_CONTENT, OK}
import play.api.libs.json.{JsValue, Json}
import play.api.libs.ws.JsonBodyWritables.writeableOf_JsValue
import uk.gov.hmrc.http.HttpReads.Implicits.*
import uk.gov.hmrc.http.client.HttpClientV2
import uk.gov.hmrc.http.{HeaderCarrier, HttpResponse, StringContextOps}
import uk.gov.hmrc.securitiestransferchargeregfrontend.clients.*
import uk.gov.hmrc.securitiestransferchargeregfrontend.clients.registration.IndividualRegistrationDetails.format
import uk.gov.hmrc.securitiestransferchargeregfrontend.clients.registration.RegistrationResponse.RegistrationSuccessful
import uk.gov.hmrc.securitiestransferchargeregfrontend.config.FrontendAppConfig

import java.util.UUID
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait RegistrationClient:
  def viewSubscription(subscriptionId: String)(implicit hc: HeaderCarrier): Future[ViewSubscriptionResult]
  def register(individualRegistrationDetails: IndividualRegistrationDetails)(implicit hc: HeaderCarrier): Future[RegistrationResult]
  def subscribe(subscriptionDetails: SubscriptionDetails)(implicit hc: HeaderCarrier): Future[SubscriptionResult]
  def enrolIndividual(enrolmentDetails: IndividualEnrolmentDetails)(implicit hc: HeaderCarrier): Future[EnrolmentResult]
  def enrolOrganisation(enrolmentDetails: OrganisationEnrolmentDetails)(implicit hc: HeaderCarrier): Future[EnrolmentResult]

class RegistrationClientImpl @Inject()(
                                        http: HttpClientV2,
                                        config: FrontendAppConfig
                                      )(implicit ec: ExecutionContext) extends RegistrationClient with Logging {

  private def correlationId: String = UUID.randomUUID().toString

  private def headers: Seq[(String, String)] = Seq("correlation-id" -> correlationId)

  override def viewSubscription(
                                 subscriptionId: String
                               )(implicit hc: HeaderCarrier): Future[ViewSubscriptionResult] = {

    val url = url"${config.viewSubscriptionBaseUrl}/$subscriptionId"

    http.get(url)
      .setHeader(headers: _*)
      .execute[HttpResponse]
      .map { resp =>
        resp.status match {
          case OK =>
            Json.parse(resp.body).validate[ViewSubscriptionResponseDto].asEither match {
              case Right(dto) =>
                Right(Some(dto))
              case Left(errors) =>
                val msg = s"RegistrationClient.viewSubscription: Failed to parse 200 response. errors=$errors body=${resp.body}"
                logger.error(msg)
                Left(SubscriptionServerError(msg))
            }

          case NOT_FOUND =>
            Right(None)

          case status =>
            Left(SubscriptionServerError(s"RegistrationClient.viewSubscription: unexpected status=$status body=${resp.body}"))
        }
      }
      .recover { case NonFatal(e) =>
        val msg = s"RegistrationClient.viewSubscription: exception calling BE: ${e.getMessage}"
        logger.error(msg, e)
        Left(SubscriptionServerError(msg))
      }
  }

  override def register(
                         individualRegistrationDetails: IndividualRegistrationDetails
                       )(implicit hc: HeaderCarrier): Future[RegistrationResult] = {

    val url = url"${config.registerIndividualBackendUrl}"

    http
      .post(url)
      .withBody(Json.toJson(individualRegistrationDetails): JsValue)
      .execute[HttpResponse]
      .map { resp =>
        resp.status match {

          case OK =>
            Json.parse(resp.body).validate[RegisterIndividualResponseDto].asEither match {
              case Right(dto) =>
                Right(RegistrationSuccessful(dto.safeId))

              case Left(errors) =>
                val msg =
                  s"RegistrationClient.register: Could not parse OK response. errors=$errors body=${resp.body}"
                logger.error(msg)
                Left(RegistrationServerError(msg))
            }

          case BAD_REQUEST =>
            Left(
              RegistrationClientError(
                s"RegistrationClient.register: 400 from BE. body=${resp.body}"
              )
            )

          case status =>
            Left(
              RegistrationServerError(
                s"RegistrationClient.register: unexpected status=$status body=${resp.body}"
              )
            )
        }
      }
      .recover {
        case NonFatal(e) =>
          val msg =
            s"RegistrationClient.register: exception calling BE: ${e.getMessage}"
          logger.error(msg, e)
          Left(RegistrationServerError(msg))
      }
  }

  override def subscribe(
                          subscriptionDetails: SubscriptionDetails
                        )(implicit hc: HeaderCarrier): Future[SubscriptionResult] = {

    val url = url"${config.subscribeUrl}"

    http
      .post(url)
      .withBody(Json.toJson(subscriptionDetails))
      .setHeader(headers: _*)
      .execute[HttpResponse]
      .map(handleResponse)
      .recover {
        case NonFatal(e) =>
          val msg =
            s"SubscriptionClient.subscribe: exception calling BE: ${e.getMessage}"
          logger.error(msg, e)
          Left(SubscriptionServerError(msg))
      }
  }

  private def handleResponse(resp: HttpResponse): SubscriptionResult =
    resp.status match {
      case CREATED =>
        parseSuccessResponse(resp.body)
      case BAD_REQUEST =>
        Left(SubscriptionClientError(s"SubscriptionClient.subscribe: 400 from BE. body=${resp.body}"))
      case status =>
        Left(
          SubscriptionServerError(s"SubscriptionClient.subscribe: unexpected status=$status body=${resp.body}"))
    }

  private def parseSuccessResponse(body: String): SubscriptionResult =
    Json
      .parse(body)
      .validate[SubscriptionResponseDto]
      .fold(
        errs =>
          val msg =
            s"SubscriptionClient.subscribe: Could not parse OK response. errs=$errs body=$body"
          Left(SubscriptionServerError(msg)),
        dto =>
          Right(
            SubscriptionResponse.SubscriptionSuccessful(dto.subscriptionId)
          )
      )
  
  

  override def enrolIndividual(
                                enrolmentDetails: IndividualEnrolmentDetails
                              )(implicit hc: HeaderCarrier): Future[EnrolmentResult] = {

    val url = url"${config.enrolIndividualBackendUrl}"

    http.post(url)
      .withBody(Json.toJson(enrolmentDetails): JsValue)
      .execute[HttpResponse]
      .map { resp =>
        resp.status match {
          case NO_CONTENT | OK =>
            Right(EnrolmentResponse.EnrolmentSuccessful)

          case BAD_REQUEST =>
            Left(EnrolmentClientError(s"400 from BE. body=${resp.body}"))

          case status =>
            Left(EnrolmentServerError(s"unexpected status=$status body=${resp.body}"))
        }
      }
      .recover { case NonFatal(e) =>
        Left(EnrolmentServerError(s"exception calling BE: ${e.getMessage}"))
      }
  }

  override def enrolOrganisation(
                                  enrolmentDetails: OrganisationEnrolmentDetails
                                )(implicit hc: HeaderCarrier): Future[EnrolmentResult] = {

    val url = url"${config.enrolOrganisationBackendUrl}"

    http.post(url)
      .withBody(Json.toJson(enrolmentDetails): JsValue)
      .execute[HttpResponse]
      .map { resp =>
        resp.status match {
          case NO_CONTENT | OK =>
            Right(EnrolmentResponse.EnrolmentSuccessful)

          case BAD_REQUEST =>
            Left(EnrolmentClientError(s"400 from BE. body=${resp.body}"))

          case status =>
            Left(EnrolmentServerError(s"unexpected status=$status body=${resp.body}"))
        }
      }
      .recover { case NonFatal(e) =>
        Left(EnrolmentServerError(s"exception calling BE: ${e.getMessage}"))
      }
  }

}