package com.apnileap.scheduler.exception;
import com.apnileap.scheduler.dto.ApiErrorResponse;
import com.apnileap.scheduler.dto.Meta;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiErrorResponse> handleValidation(MethodArgumentNotValidException ex, HttpServletRequest request) {
        String correlationId = (String) request.getAttribute("correlationId");
        ApiErrorResponse.ErrorDetail error = ApiErrorResponse.ErrorDetail.builder()
                .code("VALIDATION_ERROR")
                .message("Validation failed")
                .details(ex.getBindingResult().getFieldErrors().stream()
                        .map(err -> err.getField() + ": " + err.getDefaultMessage())
                        .collect(Collectors.toList()))
                .build();
        return buildResponse(HttpStatus.BAD_REQUEST, error, correlationId);
    }

    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<ApiErrorResponse> handleBusinessException(BusinessException ex, HttpServletRequest request) {
        String correlationId = (String) request.getAttribute("correlationId");
        ApiErrorResponse.ErrorDetail error = ApiErrorResponse.ErrorDetail.builder()
                .code(ex.getCode())
                .message(ex.getMessage())
                .build();
        HttpStatus status = switch (ex.getCode()) {
            case "NOT_FOUND" -> HttpStatus.NOT_FOUND;
            case "MISSING_IDEMPOTENCY_KEY", "VALIDATION_ERROR", "UNKNOWN_POLICY" -> HttpStatus.BAD_REQUEST;
            default -> HttpStatus.CONFLICT;
        };
        return buildResponse(status, error, correlationId);
    }
    
    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ApiErrorResponse> handleNotFound(NoResourceFoundException ex, HttpServletRequest request) {
        String correlationId = (String) request.getAttribute("correlationId");
        ApiErrorResponse.ErrorDetail error = ApiErrorResponse.ErrorDetail.builder()
                .code("NOT_FOUND")
                .message("No API at this URL. Use /api/v1/health, /swagger-ui.html, or /api/v1/backup/jobs")
                .build();
        return buildResponse(HttpStatus.NOT_FOUND, error, correlationId);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResponse> handleGeneric(Exception ex, HttpServletRequest request) {
        String correlationId = (String) request.getAttribute("correlationId");
        ApiErrorResponse.ErrorDetail error = ApiErrorResponse.ErrorDetail.builder()
                .code("INTERNAL_SERVER_ERROR")
                .message(ex.getMessage())
                .build();
        return buildResponse(HttpStatus.INTERNAL_SERVER_ERROR, error, correlationId);
    }
    
    private ResponseEntity<ApiErrorResponse> buildResponse(HttpStatus status, ApiErrorResponse.ErrorDetail error, String correlationId) {
        return ResponseEntity.status(status).body(ApiErrorResponse.builder()
                .error(error)
                .meta(Meta.builder().correlation_id(correlationId).build())
                .build());
    }
}
