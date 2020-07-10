package com.file.s3.controller;

import com.file.s3.service.impl.ConsultantServiceImpl;
import com.file.s3.utils.enums.DocumentType;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import static org.springframework.web.bind.annotation.RequestMethod.*;

@RestController
@RequestMapping("/consultant")
@CrossOrigin(origins = "*", allowedHeaders = "*", methods = {GET, POST, PUT})
@Api(tags = "Consultant Service", description = "Consultant REST Endpoints")
public class ConsultantController {

    @Autowired
    private ConsultantServiceImpl consultantService;

    @PutMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public String updateConsultantDocuments(
            @RequestParam(name = "doc-type") DocumentType type,
            @RequestParam(name = "document") MultipartFile file) {

        return consultantService.updateDocs(file, type);
    }

}
