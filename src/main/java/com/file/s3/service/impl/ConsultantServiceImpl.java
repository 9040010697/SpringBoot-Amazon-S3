package com.file.s3.service.impl;

import com.file.s3.utils.enums.DocumentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.UUID;

@Service
public class ConsultantServiceImpl {

    @Autowired
    private AmazonS3ClientImpl amazonS3Client;

    public String updateDocs(MultipartFile file, DocumentType documentType) {
        return amazonS3Client.saveDocument(file, documentType, UUID.randomUUID().toString());
    }
}
